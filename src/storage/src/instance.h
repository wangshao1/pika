//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_INSTANCE_H_
#define SRC_INSTANCE_H_

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

#include "pstd/include/pika_codis_slot.h"
#include "src/lock_mgr.h"
#include "src/lru_cache.h"
#include "src/mutex_impl.h"
#include "src/custom_comparator.h"
#include "src/type_iterator.h"
#include "storage/storage.h"
#include "storage/storage_define.h"
#include "pstd/include/env.h"
#include "src/debug.h"

#define SPOP_COMPACT_THRESHOLD_COUNT 500
#define SPOP_COMPACT_THRESHOLD_DURATION (1000 * 1000)  // 1000ms

namespace storage {
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class Instance {
 public:
  Instance(Storage* storage, int32_t index);
  virtual ~Instance();

  rocksdb::DB* GetDB() { return db_; }

  int GetIndex() const {return index_;}

  Status SetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options);
  void SetWriteWalOptions(const bool is_wal_disable);

  // Common Commands
  Status Open(const StorageOptions& storage_options, const std::string& db_path);

  virtual Status CompactRange(const DataType& option_type, const rocksdb::Slice* begin, const rocksdb::Slice* end,
                              const ColumnFamilyType& type = kMetaAndData);

  virtual Status GetProperty(const std::string& property, uint64_t* out);

  Status ScanKeyNum(std::vector<KeyInfo>* key_info);
  Status ScanStringsKeyNum(KeyInfo* key_info);
  Status ScanHashesKeyNum(KeyInfo* key_info);
  Status ScanListsKeyNum(KeyInfo* key_info);
  Status ScanZsetsKeyNum(KeyInfo* key_info);
  Status ScanSetsKeyNum(KeyInfo* key_info);

  virtual Status StringsPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status ListsPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status HashesPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status ZsetsPKPatternMatchDel(const std::string& pattern, int32_t* ret);
  virtual Status SetsPKPatternMatchDel(const std::string& pattern, int32_t* ret);

  // Keys Commands
  virtual Status StringsExpire(const Slice& key, int32_t ttl);
  virtual Status HashesExpire(const Slice& key, int32_t ttl);
  virtual Status ListsExpire(const Slice& key, int32_t ttl);
  virtual Status ZsetsExpire(const Slice& key, int32_t ttl);
  virtual Status SetsExpire(const Slice& key, int32_t ttl);

  virtual Status StringsDel(const Slice& key);
  virtual Status HashesDel(const Slice& key);
  virtual Status ListsDel(const Slice& key);
  virtual Status ZsetsDel(const Slice& key);
  virtual Status SetsDel(const Slice& key);

  virtual Status StringsExpireat(const Slice& key, int32_t timestamp);
  virtual Status HashesExpireat(const Slice& key, int32_t timestamp);
  virtual Status ListsExpireat(const Slice& key, int32_t timestamp);
  virtual Status SetsExpireat(const Slice& key, int32_t timestamp);
  virtual Status ZsetsExpireat(const Slice& key, int32_t timestamp);

  virtual Status StringsPersist(const Slice& key);
  virtual Status HashesPersist(const Slice& key);
  virtual Status ListsPersist(const Slice& key);
  virtual Status ZsetsPersist(const Slice& key);
  virtual Status SetsPersist(const Slice& key);

  virtual Status StringsTTL(const Slice& key, int64_t* timestamp);
  virtual Status HashesTTL(const Slice& key, int64_t* timestamp);
  virtual Status ListsTTL(const Slice& key, int64_t* timestamp);
  virtual Status ZsetsTTL(const Slice& key, int64_t* timestamp);
  virtual Status SetsTTL(const Slice& key, int64_t* timestamp);

  // Strings Commands
  Status Append(const Slice& key, const Slice& value, int32_t* ret);
  Status BitCount(const Slice& key, int64_t start_offset, int64_t end_offset, int32_t* ret, bool have_range);
  Status BitOp(BitOpType op, const std::string& dest_key, const std::vector<std::string>& src_keys, std::string &value_to_dest, int64_t* ret);
  Status Decrby(const Slice& key, int64_t value, int64_t* ret);
  Status Get(const Slice& key, std::string* value);
  Status GetBit(const Slice& key, int64_t offset, int32_t* ret);
  Status Getrange(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret);
  Status GetSet(const Slice& key, const Slice& value, std::string* old_value);
  Status Incrby(const Slice& key, int64_t value, int64_t* ret);
  Status Incrbyfloat(const Slice& key, const Slice& value, std::string* ret);
  Status MSet(const std::vector<KeyValue>& kvs);
  Status MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret);
  Status Set(const Slice& key, const Slice& value);
  Status Setxx(const Slice& key, const Slice& value, int32_t* ret, int32_t ttl = 0);
  Status SetBit(const Slice& key, int64_t offset, int32_t value, int32_t* ret);
  Status Setex(const Slice& key, const Slice& value, int32_t ttl);
  Status Setnx(const Slice& key, const Slice& value, int32_t* ret, int32_t ttl = 0);
  Status Setvx(const Slice& key, const Slice& value, const Slice& new_value, int32_t* ret, int32_t ttl = 0);
  Status Delvx(const Slice& key, const Slice& value, int32_t* ret);
  Status Setrange(const Slice& key, int64_t start_offset, const Slice& value, int32_t* ret);
  Status Strlen(const Slice& key, int32_t* len);

  Status BitPos(const Slice& key, int32_t bit, int64_t* ret);
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t* ret);
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t end_offset, int64_t* ret);
  Status PKSetexAt(const Slice& key, const Slice& value, int32_t timestamp);

  // Hash Commands
  Status HDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret);
  Status HExists(const Slice& key, const Slice& field);
  Status HGet(const Slice& key, const Slice& field, std::string* value);
  Status HGetall(const Slice& key, std::vector<FieldValue>* fvs);
  Status HIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret);
  Status HIncrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value);
  Status HKeys(const Slice& key, std::vector<std::string>* fields);
  Status HLen(const Slice& key, int32_t* ret);
  Status HMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss);
  Status HMSet(const Slice& key, const std::vector<FieldValue>& fvs);
  Status HSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res);
  Status HSetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret);
  Status HVals(const Slice& key, std::vector<std::string>* values);
  Status HStrlen(const Slice& key, const Slice& field, int32_t* len);
  Status HScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<FieldValue>* field_values, int64_t* next_cursor);
  Status HScanx(const Slice& key, const std::string& start_field, const std::string& pattern, int64_t count,
                std::vector<FieldValue>* field_values, std::string* next_field);
  Status PKHScanRange(const Slice& key, const Slice& field_start, const std::string& field_end, const Slice& pattern,
                      int32_t limit, std::vector<FieldValue>* field_values, std::string* next_field);
  Status PKHRScanRange(const Slice& key, const Slice& field_start, const std::string& field_end, const Slice& pattern,
                       int32_t limit, std::vector<FieldValue>* field_values, std::string* next_field);

  Status SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys);
  Status SetSmallCompactionThreshold(size_t small_compaction_threshold);
  void GetRocksDBInfo(std::string &info, const char *prefix);

  // Sets Commands
  Status SAdd(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status SCard(const Slice& key, int32_t* ret);
  Status SDiff(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SDiffstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SInter(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SInterstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);
  Status StoreValue(const Slice& destination, const std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SIsmember(const Slice& key, const Slice& member, int32_t* ret);
  Status SMembers(const Slice& key, std::vector<std::string>* members);
  Status SMove(const Slice& source, const Slice& destination, const Slice& member, int32_t* ret);
  Status SPop(const Slice& key, std::vector<std::string>* members, bool* need_compact, int64_t cnt);
  Status SRandmember(const Slice& key, int32_t count, std::vector<std::string>* members);
  Status SRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status SUnion(const std::vector<std::string>& keys, std::vector<std::string>* members);
  Status SUnionstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);
  Status SScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<std::string>* members, int64_t* next_cursor);
  Status AddAndGetSpopCount(const std::string& key, uint64_t* count);
  Status ResetSpopCount(const std::string& key);

  // Lists commands
  Status LIndex(const Slice& key, int64_t index, std::string* element);
  Status LInsert(const Slice& key, const BeforeOrAfter& before_or_after, const std::string& pivot,
                 const std::string& value, int64_t* ret);
  Status LLen(const Slice& key, uint64_t* len);
  Status LPop(const Slice& key, int64_t count, std::vector<std::string>* elements);
  Status LPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret);
  Status LPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len);
  Status LRange(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret);
  Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret);
  Status LSet(const Slice& key, int64_t index, const Slice& value);
  Status LTrim(const Slice& key, int64_t start, int64_t stop);
  Status RPop(const Slice& key, int64_t count, std::vector<std::string>* elements);
  Status RPoplpush(const Slice& source, const Slice& destination, std::string* element);
  Status RPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret);
  Status RPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len);

  // Zsets Commands
  Status ZAdd(const Slice& key, const std::vector<ScoreMember>& score_members, int32_t* ret);
  Status ZCard(const Slice& key, int32_t* card);
  Status ZCount(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret);
  Status ZIncrby(const Slice& key, const Slice& member, double increment, double* ret);
  Status ZRange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members);
  Status ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int64_t count,
                       int64_t offset, std::vector<ScoreMember>* score_members);
  Status ZRank(const Slice& key, const Slice& member, int32_t* rank);
  Status ZRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret);
  Status ZRemrangebyrank(const Slice& key, int32_t start, int32_t stop, int32_t* ret);
  Status ZRemrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret);
  Status ZRevrange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members);
  Status ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int64_t count,
                          int64_t offset, std::vector<ScoreMember>* score_members);
  Status StoreValue(const Slice& destination, std::map<std::string, double>& value_to_dest, int32_t* ret);
  Status StoreValue(const Slice& destination, std::vector<ScoreMember>& score_members, int32_t* ret);
  Status ZRevrank(const Slice& key, const Slice& member, int32_t* rank);
  Status ZScore(const Slice& key, const Slice& member, double* score);
  Status ZGetAll(const Slice& key, double weight, std::map<std::string, double>* value_to_dest);
  Status ZUnionstore(const Slice& destination, const std::vector<std::string>& keys, const std::vector<double>& weights,
                     AGGREGATE agg, std::map<std::string, double>& value_to_dest, int32_t* ret);
  Status ZInterstore(const Slice& destination, const std::vector<std::string>& keys, const std::vector<double>& weights,
                     AGGREGATE agg, std::vector<ScoreMember>& value_to_dest, int32_t* ret);
  Status ZRangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                     std::vector<std::string>* members);
  Status ZLexcount(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                   int32_t* ret);
  Status ZRemrangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                        int32_t* ret);
  Status ZScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<ScoreMember>* score_members, int64_t* next_cursor);
  Status ZPopMax(const Slice& key, int64_t count, std::vector<ScoreMember>* score_members);
  Status ZPopMin(const Slice& key, int64_t count, std::vector<ScoreMember>* score_members);

  void ScanDatabase();
  void ScanStrings();
  void ScanHashes();
  void ScanLists();
  void ScanZsets();
  void ScanSets();

  TypeIterator* CreateIterator(const DataType& type, const std::string& pattern, const Slice* lower_bound, const Slice* upper_bound) {
    return CreateIterator(DataTypeTag[type], pattern, lower_bound, upper_bound);
  }

  TypeIterator* CreateIterator(const char& type, const std::string& pattern, const Slice* lower_bound, const Slice* upper_bound) {
    rocksdb::ReadOptions options;
    options.fill_cache = false;
    options.iterate_lower_bound = lower_bound;
    options.iterate_upper_bound = upper_bound;
    switch (type) {
      case 'k':
        return new StringsIterator(options, db_, handles_[kStringsCF], pattern);
        break;
      case 'h':
        return new HashesIterator(options, db_, handles_[kHashesMetaCF], pattern);
        break;
      case 's':
        return new SetsIterator(options, db_, handles_[kSetsMetaCF], pattern);
        break;
      case 'l':
        return new ListsIterator(options, db_, handles_[kListsMetaCF], pattern);
        break;
      case 'z':
        return new ZsetsIterator(options, db_, handles_[kZsetsMetaCF], pattern);
        break;
      default:
        LOG(WARNING) << "Invalid datatype to create iterator";
        return nullptr;
    }
    return nullptr;
  }

private:
  int32_t index_ = 0;
  Storage* const storage_;
  std::shared_ptr<LockMgr> lock_mgr_;
  rocksdb::DB* db_ = nullptr;
  //TODO(wangshaoyi): seperate env for each rocksdb instance
  //std::unique_ptr<rocksdb::Env> env_;

  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;

  // For Scan
  std::unique_ptr<LRUCache<std::string, std::string>> scan_cursors_store_;
  std::unique_ptr<LRUCache<std::string, size_t>> spop_counts_store_;

  Status GetScanStartPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor, std::string* start_point);
  Status StoreScanNextPoint(const DataType& type, const Slice& key, const Slice& pattern, int64_t cursor, const std::string& next_point);

  // For Statistics
  std::atomic<size_t> small_compaction_threshold_;
  std::unique_ptr<LRUCache<std::string, size_t>> statistics_store_;

  Status UpdateSpecificKeyStatistics(const DataType& dtype, const std::string& key, size_t count);
  Status AddCompactKeyTaskIfNeeded(const DataType& dtype, const std::string& key, size_t total);
};

}  //  namespace storage
#endif  //  SRC_INSTANCE_H_
