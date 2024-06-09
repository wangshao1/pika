//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>

#include "rocksdb/env.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "cloud/filename.h"

#include "src/redis.h"
#include "rocksdb/options.h"
#include "src/strings_filter.h"
#include "src/lists_filter.h"
#include "src/base_filter.h"
#include "src/zsets_filter.h"

#include "pstd/include/pstd_defer.h"

namespace storage {
const rocksdb::Comparator* ListsDataKeyComparator() {
  static ListsDataKeyComparatorImpl ldkc;
  return &ldkc;
}

rocksdb::Comparator* ZSetsScoreKeyComparator() {
  static ZSetsScoreKeyComparatorImpl zsets_score_key_compare;
  return &zsets_score_key_compare;
}

Redis::Redis(Storage* const s, int32_t index, std::shared_ptr<pstd::WalWriter> wal_writer)
    : storage_(s), index_(index),
      lock_mgr_(std::make_shared<LockMgr>(1000, 0, std::make_shared<MutexFactoryImpl>())),
      small_compaction_threshold_(5000),
      small_compaction_duration_threshold_(10000),
      wal_writer_(wal_writer) {
  statistics_store_ = std::make_unique<LRUCache<std::string, KeyStatistics>>();
  scan_cursors_store_ = std::make_unique<LRUCache<std::string, std::string>>();
  spop_counts_store_ = std::make_unique<LRUCache<std::string, size_t>>();
  default_compact_range_options_.exclusive_manual_compaction = false;
  default_compact_range_options_.change_level = true;
  default_write_options_.disableWAL = true;
  spop_counts_store_->SetCapacity(1000);
  scan_cursors_store_->SetCapacity(5000);
  //env_ = rocksdb::Env::Instance();
#ifdef USE_S3
  log_listener_ = std::make_shared<LogListener>(index_, this, wal_writer);
#endif
  handles_.clear();
}

Redis::~Redis() {
  Close();
}

void Redis::Close() {
  rocksdb::CancelAllBackgroundWork(db_, true);
  std::vector<rocksdb::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
  // delete env_;
  delete db_;

  if (default_compact_range_options_.canceled) {
    delete default_compact_range_options_.canceled;
  }
#ifdef USE_S3
  opened_ = false;
#endif
}

Status Redis::FlushDBAtSlave() {
  Close();
  pstd::DeleteDir(db_path_);
  return Open(storage_options_, db_path_);
}

Status Redis::FlushDB() {
  rocksdb::CancelAllBackgroundWork(db_, true);
  std::string s3_bucket = storage_options_.cloud_fs_options.dest_bucket.GetBucketName();
  std::string local_dbid;
  auto s = ReadFileToString(cfs_->GetBaseFileSystem().get(), rocksdb::IdentityFileName(db_path_), &local_dbid);
  LOG(INFO) << "local_dbid: " << local_dbid << " status: " << s.ToString();
  if (!s.ok()) {
    return s;
  }
  s = cfs_->DeleteDbid(s3_bucket, local_dbid);
  LOG(INFO) << " deletedbid status: " << s.ToString();
  if (!s.ok()) {
    return s;
  }
  s = cfs_->DeleteCloudObject(s3_bucket, MakeCloudManifestFile(db_path_, ""));
  LOG(INFO) << "deletecloudmanifestfromdest tatus: " << s.ToString(); 
  if (!s.ok()) {
    return s;
  }
  s = cfs_->DeleteCloudObject(s3_bucket, rocksdb::IdentityFileName(db_path_));
  LOG(INFO) << "deleteidentityfile status: " << s.ToString(); 
  if (!s.ok()) {
    return s;
  }
  cfs_->SwitchMaster(false);
  Close();
  pstd::DeleteDir(db_path_);
  Open(storage_options_, db_path_);
  wal_writer_->Put("flushdb", 0/*db_id*/, index_, static_cast<uint32_t>(RocksDBRecordType::kFlushDB));
  return s;
}

Status Redis::Open(const StorageOptions& tmp_storage_options, const std::string& db_path) {

  StorageOptions storage_options(tmp_storage_options);
#ifdef USE_S3
  db_path_ = db_path;
  storage_options_ = tmp_storage_options;
  storage_options_.cloud_fs_options.dest_bucket.SetObjectPath(db_path_);
  storage_options_.cloud_fs_options.src_bucket.SetObjectPath(db_path_);
  storage_options.cloud_fs_options.roll_cloud_manifest_on_open = true;
  storage_options.cloud_fs_options.resync_on_open = true;
  storage_options.cloud_fs_options.resync_manifest_on_open = true;
  storage_options.cloud_fs_options.skip_dbid_verification = true;
  storage_options.cloud_fs_options.sst_file_cache = rocksdb::NewLRUCache(storage_options_.sst_cache_size_, 0/*num_shard_bits*/);
  storage_options.options.replication_log_listener = log_listener_;

  is_master_.store(tmp_storage_options.cloud_fs_options.is_master);
  if (!tmp_storage_options.cloud_fs_options.is_master) {
    storage_options.options.disable_auto_flush = true;
    storage_options.options.disable_auto_compactions = true;
  }
  storage_options.options.atomic_flush = true;
  storage_options.options.avoid_flush_during_shutdown = true;
#endif

  statistics_store_->SetCapacity(storage_options.statistics_max_size);
  small_compaction_threshold_ = storage_options.small_compaction_threshold;

  rocksdb::BlockBasedTableOptions table_ops(storage_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

  rocksdb::Options db_ops(storage_options.options);
  db_ops.create_missing_column_families = true;
  db_ops.listeners.emplace_back(new RocksDBEventListener(index_));
  // db_ops.env = env_;

  // string column-family options
  rocksdb::ColumnFamilyOptions string_cf_ops(storage_options.options);
  string_cf_ops.compaction_filter_factory = std::make_shared<StringsFilterFactory>();

  rocksdb::BlockBasedTableOptions string_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    string_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  string_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(string_table_ops));


  // hash column-family options
  rocksdb::ColumnFamilyOptions hash_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions hash_data_cf_ops(storage_options.options);
  hash_meta_cf_ops.compaction_filter_factory = std::make_shared<HashesMetaFilterFactory>();
  hash_data_cf_ops.compaction_filter_factory = std::make_shared<HashesDataFilterFactory>(&db_, &handles_, kHashesMetaCF);

  rocksdb::BlockBasedTableOptions hash_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions hash_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    hash_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    hash_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  hash_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(hash_meta_cf_table_ops));
  hash_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(hash_data_cf_table_ops));

  // list column-family options
  rocksdb::ColumnFamilyOptions list_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions list_data_cf_ops(storage_options.options);
  list_meta_cf_ops.compaction_filter_factory = std::make_shared<ListsMetaFilterFactory>();
  list_data_cf_ops.compaction_filter_factory = std::make_shared<ListsDataFilterFactory>(&db_, &handles_, kListsMetaCF);
  list_data_cf_ops.comparator = ListsDataKeyComparator();

  rocksdb::BlockBasedTableOptions list_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions list_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    list_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    list_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  list_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(list_meta_cf_table_ops));
  list_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(list_data_cf_table_ops));

  // set column-family options
  rocksdb::ColumnFamilyOptions set_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions set_data_cf_ops(storage_options.options);
  set_meta_cf_ops.compaction_filter_factory = std::make_shared<SetsMetaFilterFactory>();
  set_data_cf_ops.compaction_filter_factory = std::make_shared<SetsMemberFilterFactory>(&db_, &handles_, kSetsMetaCF);

  rocksdb::BlockBasedTableOptions set_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions set_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    set_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    set_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  set_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(set_meta_cf_table_ops));
  set_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(set_data_cf_table_ops));

  // zset column-family options
  rocksdb::ColumnFamilyOptions zset_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions zset_data_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions zset_score_cf_ops(storage_options.options);
  zset_meta_cf_ops.compaction_filter_factory = std::make_shared<ZSetsMetaFilterFactory>();
  zset_data_cf_ops.compaction_filter_factory = std::make_shared<ZSetsDataFilterFactory>(&db_, &handles_, kZsetsMetaCF);
  zset_score_cf_ops.compaction_filter_factory = std::make_shared<ZSetsScoreFilterFactory>(&db_, &handles_, kZsetsMetaCF);
  zset_score_cf_ops.comparator = ZSetsScoreKeyComparator();

  rocksdb::BlockBasedTableOptions zset_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions zset_data_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions zset_score_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    zset_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    zset_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    zset_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  zset_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_meta_cf_table_ops));
  zset_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_data_cf_table_ops));
  zset_score_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(zset_score_cf_table_ops));

  // stream column-family options
  rocksdb::ColumnFamilyOptions stream_meta_cf_ops(storage_options.options);
  rocksdb::ColumnFamilyOptions stream_data_cf_ops(storage_options.options);

  rocksdb::BlockBasedTableOptions stream_meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions stream_data_cf_table_ops(table_ops);
  if (!storage_options.share_block_cache && storage_options.block_cache_size > 0) {
    stream_meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
    stream_data_cf_table_ops.block_cache = rocksdb::NewLRUCache(storage_options.block_cache_size);
  }
  stream_meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(stream_meta_cf_table_ops));
  stream_data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(stream_data_cf_table_ops));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, string_cf_ops);
  // hash CF
  column_families.emplace_back("hash_meta_cf", hash_meta_cf_ops);
  column_families.emplace_back("hash_data_cf", hash_data_cf_ops);
  // set CF
  column_families.emplace_back("set_meta_cf", set_meta_cf_ops);
  column_families.emplace_back("set_data_cf", set_data_cf_ops);
  // list CF
  column_families.emplace_back("list_meta_cf", list_meta_cf_ops);
  column_families.emplace_back("list_data_cf", list_data_cf_ops);
  // zset CF
  column_families.emplace_back("zset_meta_cf", zset_meta_cf_ops);
  column_families.emplace_back("zset_data_cf", zset_data_cf_ops);
  column_families.emplace_back("zset_score_cf", zset_score_cf_ops);
  // stream CF
  column_families.emplace_back("stream_meta_cf", stream_meta_cf_ops);
  column_families.emplace_back("stream_data_cf", stream_data_cf_ops);

#ifdef USE_S3
  Status s = OpenCloudEnv(storage_options.cloud_fs_options, db_path);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to create AWS S3 cloud environment";
    return s;
  }
  db_ops.env = cloud_env_.get();
  s = rocksdb::DBCloud::Open(db_ops, db_path, column_families, "", 0, &handles_, &db_);
  if (s.ok()) {
    opened_ = true;
  }
  return s;

#else
  auto s = rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
  opened_ = true;
  return s;
#endif
}

Status Redis::GetScanStartPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point) {
  std::string index_key;
  index_key.append(1, DataTypeTag[type]);
  index_key.append("_");
  index_key.append(key.ToString());
  index_key.append("_");
  index_key.append(pattern.ToString());
  index_key.append("_");
  index_key.append(std::to_string(cursor));
  return scan_cursors_store_->Lookup(index_key, start_point);
}

Status Redis::StoreScanNextPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor,
                                 const std::string& next_point) {
  std::string index_key;
  index_key.append(1, DataTypeTag[type]);
  index_key.append("_");
  index_key.append(key.ToString());
  index_key.append("_");
  index_key.append(pattern.ToString());
  index_key.append("_");
  index_key.append(std::to_string(cursor));
  return scan_cursors_store_->Insert(index_key, next_point);
}

Status Redis::SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys) {
  statistics_store_->SetCapacity(max_cache_statistic_keys);
  return Status::OK();
}

Status Redis::CompactRange(const DataType& dtype, const rocksdb::Slice* begin, const rocksdb::Slice* end, const ColumnFamilyType& type) {
  Status s;
  switch (dtype) {
    case DataType::kStrings:
      s = db_->CompactRange(default_compact_range_options_, begin, end);
      break;
    case DataType::kHashes:
      if (type == kMeta || type == kMetaAndData) {
        s = db_->CompactRange(default_compact_range_options_, handles_[kHashesMetaCF], begin, end);
      }
      if (s.ok() && (type == kData || type == kMetaAndData)) {
        s = db_->CompactRange(default_compact_range_options_, handles_[kHashesDataCF], begin, end);
      }
      break;
    case DataType::kSets:
      if (type == kMeta || type == kMetaAndData) {
        db_->CompactRange(default_compact_range_options_, handles_[kSetsMetaCF], begin, end);
      }
      if (s.ok() && (type == kData || type == kMetaAndData)) {
        db_->CompactRange(default_compact_range_options_, handles_[kSetsDataCF], begin, end);
      }
      break;
    case DataType::kLists:
      if (type == kMeta || type == kMetaAndData) {
        s = db_->CompactRange(default_compact_range_options_, handles_[kListsMetaCF], begin, end);
      }
      if (s.ok() && (type == kData || type == kMetaAndData)) {
        s = db_->CompactRange(default_compact_range_options_, handles_[kListsDataCF], begin, end);
      }
      break;
    case DataType::kZSets:
      if (type == kMeta || type == kMetaAndData) {
        db_->CompactRange(default_compact_range_options_, handles_[kZsetsMetaCF], begin, end);
      }
      if (s.ok() && (type == kData || type == kMetaAndData)) {
        db_->CompactRange(default_compact_range_options_, handles_[kZsetsDataCF], begin, end);
        db_->CompactRange(default_compact_range_options_, handles_[kZsetsScoreCF], begin, end);
      }
      break;
    case DataType::kStreams:
      if (type == kMeta || type == kMetaAndData) {
        s = db_->CompactRange(default_compact_range_options_, handles_[kStreamsMetaCF], begin, end);
      }
      if (s.ok() && (type == kData || type == kMetaAndData)) {
        s = db_->CompactRange(default_compact_range_options_, handles_[kStreamsDataCF], begin, end);
      }
      break;
    default:
      return Status::Corruption("Invalid data type");
  }
  return s;
}

Status Redis::SetSmallCompactionThreshold(uint64_t small_compaction_threshold) {
  small_compaction_threshold_ = small_compaction_threshold;
  return Status::OK();
}

Status Redis::SetSmallCompactionDurationThreshold(uint64_t small_compaction_duration_threshold) {
  small_compaction_duration_threshold_ = small_compaction_duration_threshold;
  return Status::OK();
}

Status Redis::UpdateSpecificKeyStatistics(const DataType& dtype, const std::string& key, uint64_t count) {
  if ((statistics_store_->Capacity() != 0U) && (count != 0U) && (small_compaction_threshold_ != 0U)) {
    KeyStatistics data;
    std::string lkp_key;
    lkp_key.append(1, DataTypeTag[dtype]);
    lkp_key.append(key);
    statistics_store_->Lookup(lkp_key, &data);
    data.AddModifyCount(count);
    statistics_store_->Insert(lkp_key, data);
    AddCompactKeyTaskIfNeeded(dtype, key, data.ModifyCount(), data.AvgDuration());
  }
  return Status::OK();
}

Status Redis::UpdateSpecificKeyDuration(const DataType& dtype, const std::string& key, uint64_t duration) {
  if ((statistics_store_->Capacity() != 0U) && (duration != 0U) && (small_compaction_duration_threshold_ != 0U)) {
    KeyStatistics data;
    std::string lkp_key;
    lkp_key.append(1, DataTypeTag[dtype]);
    lkp_key.append(key);
    statistics_store_->Lookup(lkp_key, &data);
    data.AddDuration(duration);
    statistics_store_->Insert(lkp_key, data);
    AddCompactKeyTaskIfNeeded(dtype, key, data.ModifyCount(), data.AvgDuration());
  }
  return Status::OK();
}

Status Redis::AddCompactKeyTaskIfNeeded(const DataType& dtype, const std::string& key, uint64_t total, uint64_t duration) {
  if (total < small_compaction_threshold_ || duration < small_compaction_duration_threshold_) {
    return Status::OK();
  } else {
    std::string lkp_key(1, DataTypeTag[dtype]);
    lkp_key.append(key);
    storage_->AddBGTask({dtype, kCompactRange, {key}});
    statistics_store_->Remove(lkp_key);
  }
  return Status::OK();
}

Status Redis::SetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options) {
  if (option_type == OptionType::kDB) {
    return db_->SetDBOptions(options);
  }
  if (handles_.empty()) {
    return db_->SetOptions(db_->DefaultColumnFamily(), options);
  }
  Status s;
  for (auto handle : handles_) {
    s = db_->SetOptions(handle, options);
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

void Redis::GetRocksDBInfo(std::string& info, const char* prefix) {
    std::ostringstream string_stream;
    string_stream << "#" << prefix << "RocksDB" << "\r\n";

    auto write_stream_key_value=[&](const Slice& property, const char *metric) {
        uint64_t value;
        db_->GetAggregatedIntProperty(property, &value);
        string_stream << prefix << metric << ':' << value << "\r\n";
    };

    auto mapToString=[&](const std::map<std::string, std::string>& map_data, const char *prefix) {
      for (const auto& kv : map_data) {
        std::string str_data;
        str_data += kv.first + ": " + kv.second + "\r\n";
        string_stream << prefix << str_data;
      }
    };

    // memtables num
    write_stream_key_value(rocksdb::DB::Properties::kNumImmutableMemTable, "num_immutable_mem_table");
    write_stream_key_value(rocksdb::DB::Properties::kNumImmutableMemTableFlushed, "num_immutable_mem_table_flushed");
    write_stream_key_value(rocksdb::DB::Properties::kMemTableFlushPending, "mem_table_flush_pending");
    write_stream_key_value(rocksdb::DB::Properties::kNumRunningFlushes, "num_running_flushes");

    // compaction
    write_stream_key_value(rocksdb::DB::Properties::kCompactionPending, "compaction_pending");
    write_stream_key_value(rocksdb::DB::Properties::kNumRunningCompactions, "num_running_compactions");

    // background errors
    write_stream_key_value(rocksdb::DB::Properties::kBackgroundErrors, "background_errors");

    // memtables size
    write_stream_key_value(rocksdb::DB::Properties::kCurSizeActiveMemTable, "cur_size_active_mem_table");
    write_stream_key_value(rocksdb::DB::Properties::kCurSizeAllMemTables, "cur_size_all_mem_tables");
    write_stream_key_value(rocksdb::DB::Properties::kSizeAllMemTables, "size_all_mem_tables");

    // keys
    write_stream_key_value(rocksdb::DB::Properties::kEstimateNumKeys, "estimate_num_keys");

    // table readers mem
    write_stream_key_value(rocksdb::DB::Properties::kEstimateTableReadersMem, "estimate_table_readers_mem");

    // snapshot
    write_stream_key_value(rocksdb::DB::Properties::kNumSnapshots, "num_snapshots");

    // version
    write_stream_key_value(rocksdb::DB::Properties::kNumLiveVersions, "num_live_versions");
    write_stream_key_value(rocksdb::DB::Properties::kCurrentSuperVersionNumber, "current_super_version_number");

    // live data size
    write_stream_key_value(rocksdb::DB::Properties::kEstimateLiveDataSize, "estimate_live_data_size");

    // sst files
    write_stream_key_value(rocksdb::DB::Properties::kTotalSstFilesSize, "total_sst_files_size");
    write_stream_key_value(rocksdb::DB::Properties::kLiveSstFilesSize, "live_sst_files_size");

    // pending compaction bytes
    write_stream_key_value(rocksdb::DB::Properties::kEstimatePendingCompactionBytes, "estimate_pending_compaction_bytes");

    // block cache
    write_stream_key_value(rocksdb::DB::Properties::kBlockCacheCapacity, "block_cache_capacity");
    write_stream_key_value(rocksdb::DB::Properties::kBlockCacheUsage, "block_cache_usage");
    write_stream_key_value(rocksdb::DB::Properties::kBlockCachePinnedUsage, "block_cache_pinned_usage");

    // blob files
    write_stream_key_value(rocksdb::DB::Properties::kNumBlobFiles, "num_blob_files");
    write_stream_key_value(rocksdb::DB::Properties::kBlobStats, "blob_stats");
    write_stream_key_value(rocksdb::DB::Properties::kTotalBlobFileSize, "total_blob_file_size");
    write_stream_key_value(rocksdb::DB::Properties::kLiveBlobFileSize, "live_blob_file_size");

    // column family stats
    std::map<std::string, std::string> mapvalues;
    db_->rocksdb::DB::GetMapProperty(rocksdb::DB::Properties::kCFStats,&mapvalues);
    mapToString(mapvalues,prefix);
    info.append(string_stream.str());
}

void Redis::SetWriteWalOptions(const bool is_wal_disable) {
  default_write_options_.disableWAL = is_wal_disable;
}

void Redis::SetCompactRangeOptions(const bool is_canceled) {
  if (!default_compact_range_options_.canceled) {
    default_compact_range_options_.canceled = new std::atomic<bool>(is_canceled);
  } else {
    default_compact_range_options_.canceled->store(is_canceled);
  }
}

Status Redis::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  for (const auto& handle : handles_) {
    db_->GetProperty(handle, property, &value);
    *out += std::strtoull(value.c_str(), nullptr, 10);
  }
  return Status::OK();
}

Status Redis::ScanKeyNum(std::vector<KeyInfo>* key_infos) {
  key_infos->resize(5);
  rocksdb::Status s;
  s = ScanStringsKeyNum(&((*key_infos)[0]));
  if (!s.ok()) {
    return s;
  }
  s = ScanHashesKeyNum(&((*key_infos)[1]));
  if (!s.ok()) {
    return s;
  }
  s = ScanListsKeyNum(&((*key_infos)[2]));
  if (!s.ok()) {
    return s;
  }
  s = ScanZsetsKeyNum(&((*key_infos)[3]));
  if (!s.ok()) {
    return s;
  }
  s = ScanSetsKeyNum(&((*key_infos)[4]));
  if (!s.ok()) {
    return s;
  }
  s = ScanSetsKeyNum(&((*key_infos)[5]));
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

void Redis::ScanDatabase() {
  ScanStrings();
  ScanHashes();
  ScanLists();
  ScanZsets();
  ScanSets();
}

#ifdef USE_S3
Status Redis::OpenCloudEnv(rocksdb::CloudFileSystemOptions opts, const std::string& db_path) {
  std::string s3_path = db_path[0] == '.' ? db_path.substr(1) : db_path;
  opts.src_bucket.SetObjectPath(s3_path);
  opts.dest_bucket.SetObjectPath(s3_path);
  Status s = rocksdb::CloudFileSystem::NewAwsFileSystem(
    rocksdb::FileSystem::Default(),
    opts,
    nullptr,
    &cfs_
  );
  if (s.ok()) {
    std::shared_ptr<rocksdb::CloudFileSystem> cloud_fs(cfs_);
    cloud_env_ = NewCompositeEnv(cloud_fs);
  }
  return s;
}

Status Redis::ReOpenRocksDB(const storage::StorageOptions& opt) {
  Close();
  Open(opt, db_path_);
  return Status::OK();
}

Status Redis::SwitchMaster(bool is_old_master, bool is_new_master) {
  LOG(WARNING) << "switchMaster from " << (is_old_master ? "master" : "slave") 
               << " to " << (is_new_master ? "master" : "slave");
  if (is_old_master && is_new_master) {
    // Do nothing
    return Status::OK();
  }

  storage::StorageOptions storage_options(storage_options_);
  std::unordered_map<std::string, std::string> db_options;
  if (is_old_master && !is_new_master) {
    cfs_->SwitchMaster(false);
    storage_options.cloud_fs_options.is_master = false;
    is_master_.store(false);
    return ReOpenRocksDB(storage_options);
  }

  // slaveof another pika master, just reopen
  if (!is_old_master && !is_new_master) {
    storage_options.cloud_fs_options.is_master = false;
    is_master_.store(false);
    return ReOpenRocksDB(storage_options);
  }

  // slave promotes to master
  if (!is_old_master && is_new_master) {
    storage_options.cloud_fs_options.is_master = true;
    db_options["disable_auto_compactions"] = "false";
    db_options["disable_auto_flush"] = "false";
    // compare manifest_sequence
    uint64_t local_manifest_sequence = 0;
    auto s = db_->GetManifestUpdateSequence(&local_manifest_sequence);
    if (!s.ok()) {
      LOG(ERROR) << "get manifestupdatesequence error: " << s.ToString();
    }
    uint64_t remote_manifest_sequence = 0;
    cfs_->GetMaxManifestSequenceFromCurrentManifest(db_->GetName(), &remote_manifest_sequence);
    // local version behind remote, directly reopen
    if (local_manifest_sequence < remote_manifest_sequence) {
      return ReOpenRocksDB(storage_options);
    }
    // local's version cannot beyond remote's, just holding extra data in memtables
    assert(local_manifest_sequence == remote_manifest_sequence);
    storage_options_.cloud_fs_options.is_master = true;
    is_master_.store(true);

    db_->NewManifestOnNextUpdate();
    cfs_->SwitchMaster(true);
    for (const auto& cf : handles_) {
      db_->SetOptions(cf, db_options);
    }

    rocksdb::FlushOptions fops;
    fops.wait = true;
    db_->Flush(fops, handles_);
    return Status::OK();
  }
  return Status::OK();
}

bool Redis::ShouldSkip(const std::string& content) {
  rocksdb::WriteBatch batch;
  auto s = rocksdb::WriteBatchInternal::SetContents(&batch, content);
  auto sq_number = db_->GetLatestSequenceNumber();
  return rocksdb::WriteBatchInternal::Sequence(&batch) != sq_number + 1;
}

class WriteBatchHandler : public rocksdb::WriteBatch::Handler {
public:
  WriteBatchHandler(std::unordered_set<std::string>* redis_keys)
  : redis_keys_(redis_keys) {}

  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    return DeleteCF(column_family_id, key);
  }

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    switch (column_family_id) {
      case kStringsCF: {
        ParsedBaseKey pbk(key);
        redis_keys_->insert("K" + pbk.Key().ToString());
        break;
      }
      case kHashesMetaCF: {
        ParsedBaseMetaKey pbk(key);
        redis_keys_->insert("H" + pbk.Key().ToString());
        break;
      }
      case kHashesDataCF: {
        ParsedHashesDataKey pbk(key);
        redis_keys_->insert("H" + pbk.Key().ToString());
        break;
      }
      case kSetsMetaCF: {
        ParsedBaseMetaKey pbk(key);
        redis_keys_->insert("S" + pbk.Key().ToString());
        break;
      }
      case kSetsDataCF: {
        ParsedSetsMemberKey pbk(key);
        redis_keys_->insert("S" + pbk.Key().ToString());
        break;
      }
      case kListsMetaCF: {
        ParsedBaseMetaKey pbk(key);
        redis_keys_->insert("L" + pbk.Key().ToString());
        break;
      }
      case kListsDataCF: {
        ParsedListsDataKey pbk(key);
        redis_keys_->insert("L" + pbk.key().ToString());
        break;
      }
      case kZsetsMetaCF: {
        ParsedBaseMetaKey pbk(key);
        redis_keys_->insert("Z" + pbk.Key().ToString());
        break;
      }
      case kZsetsDataCF: {
        ParsedZSetsMemberKey pbk(key);
        redis_keys_->insert("Z" + pbk.Key().ToString());
        break;
      }
      case kZsetsScoreCF: {
        ParsedZSetsScoreKey pbk(key);
        redis_keys_->insert("Z" + pbk.key().ToString());
        break;
      }
      case kStreamsMetaCF: {
        LOG(INFO) << "rediscache don't cache stream type";
        break;
      }
      case kStreamsDataCF: {
        LOG(INFO) << "rediscache don't cache stream type";
        break;
      }
    }
    return Status::OK();
  }
private:
  std::unordered_set<std::string>* redis_keys_ = nullptr;
};

Status Redis::ApplyWAL(int type, const std::string& content,
    std::unordered_set<std::string>* redis_keys) {
  rocksdb::ReplicationLogRecord::Type rtype = static_cast<rocksdb::ReplicationLogRecord::Type>(type);
  rocksdb::ReplicationLogRecord rlr;
  rocksdb::DBCloud::ApplyReplicationLogRecordInfo info;
  rlr.contents = content;
  rlr.type = rtype;

  auto s = db_->ApplyReplicationLogRecord(rlr, "", nullptr, true, &info, rocksdb::DB::AR_EVICT_OBSOLETE_FILES);
  if (!s.ok()) {
    return s;
  }
  if (type != 0) {
    return s;
  }
  
  rocksdb::WriteBatch batch;
  s = rocksdb::WriteBatchInternal::SetContents(&batch, content);
  WriteBatchHandler handler(redis_keys);
  s = batch.Iterate(&handler);
  return s;
}

std::string LogListener::OnReplicationLogRecord(rocksdb::ReplicationLogRecord record) {
  Redis* redis_inst = (Redis*)inst_;
  //TODO(wangshaoyi): get from storage
  int db_id = 0;
  if (!redis_inst->opened_) {
    LOG(WARNING) << "rocksdb not opened yet, skip write binlog";
    return "0";
  }

  if (!redis_inst->IsMaster()) {
    return "0";
  }
  if (record.type != rocksdb::ReplicationLogRecord::kMemtableWrite) {
    redis_inst->cfs_->WaitPendingObjects();
  }

  auto s = wal_writer_->Put(record.contents, db_id,
      redis_inst->GetIndex(), record.type);
  if (!s.ok()) {
    LOG(ERROR) << "write binlog failed, db_id: " << db_id
               << " rocksdb_id: " << redis_inst->GetIndex();
  }
  return "";
}
#endif
}  // namespace storage
