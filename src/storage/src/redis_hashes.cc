//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/instance.h"

#include <memory>

#include <fmt/core.h>
#include <glog/logging.h>

#include "include/pika_codis_slot.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/base_data_key_format.h"
#include "storage/util.h"

namespace storage {
Status Instance::ScanHashesKeyNum(KeyInfo* key_info) {
  uint64_t keys = 0;
  uint64_t expires = 0;
  uint64_t ttl_sum = 0;
  uint64_t invaild_keys = 0;

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  int64_t curtime;
  rocksdb::Env::Default()->GetCurrentTime(&curtime);

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[2]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_hashes_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_hashes_meta_value.etime() - curtime;
      }
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->expires = expires;
  key_info->avg_ttl = (expires != 0) ? ttl_sum / expires : 0;
  key_info->invaild_keys = invaild_keys;
  return Status::OK();
}

Status Instance::HashesPKPatternMatchDel(const std::string& pattern, int32_t* ret) {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  std::string key;
  std::string meta_value;
  int32_t total_delete = 0;
  Status s;
  rocksdb::WriteBatch batch;
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[1]);
  iter->SeekToFirst();
  while (iter->Valid()) {
    key = iter->key().ToString();
    meta_value = iter->value().ToString();
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (!parsed_hashes_meta_value.IsStale() && (parsed_hashes_meta_value.count() != 0) &&
        (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0) != 0)) {
      parsed_hashes_meta_value.InitialMetaValue();
      batch.Put(handles_[1], key, meta_value);
    }
    if (static_cast<size_t>(batch.Count()) >= BATCH_DELETE_LIMIT) {
      s = db_->Write(default_write_options_, &batch);
      if (s.ok()) {
        total_delete += static_cast<int32_t>( batch.Count());
        batch.Clear();
      } else {
        *ret = total_delete;
        return s;
      }
    }
    iter->Next();
  }
  if (batch.Count() != 0U) {
    s = db_->Write(default_write_options_, &batch);
    if (s.ok()) {
      total_delete += static_cast<int32_t>(batch.Count());
      batch.Clear();
    }
  }

  *ret = total_delete;
  return s;
}

Status Instance::HDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret) {
  uint32_t statistic = 0;
  std::vector<std::string> filtered_fields;
  std::unordered_set<std::string> field_set;
  for (const auto & iter : fields) {
    const std::string& field = iter;
    if (field_set.find(field) == field_set.end()) {
      field_set.insert(field);
      filtered_fields.push_back(iter);
    }
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t del_cnt = 0;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      *ret = 0;
      return Status::OK();
    } else {
      std::string data_value;
      version = parsed_hashes_meta_value.version();
      for (const auto& field : filtered_fields) {
        HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
        s = db_->Get(read_options, handles_[2], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          statistic++;
          batch.Delete(handles_[2], hashes_data_key.Encode());
        } else if (s.IsNotFound()) {
          continue;
        } else {
          return s;
        }
      }
      *ret = del_cnt;
      parsed_hashes_meta_value.ModifyCount(-del_cnt);
      batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return Status::OK();
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status Instance::HExists(const Slice& key, const Slice& field) {
  std::string value;
  return HGet(key, field, &value);
}

Status Instance::HGet(const Slice& key, const Slice& field, std::string* value) {
  std::string meta_value;
  int32_t version = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey data_key(0/*db_id*/, slot_id, key, version, field);
      s = db_->Get(read_options, handles_[2], data_key.Encode(), value);
    }
  }
  return s;
}

Status Instance::HGetall(const Slice& key, std::vector<FieldValue>* fvs) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fvs->push_back({parsed_hashes_data_key.field().ToString(), iter->value().ToString()});
      }
      delete iter;
    }
  }
  return s;
}

Status Instance::HIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  uint32_t statistic = 0;
  std::string old_value;
  std::string meta_value;

  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  char value_buf[32] = {0};
  char meta_value_buf[4] = {0};
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.SetEtime(0);
      batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
      Int64ToStr(value_buf, 32, value);
      batch.Put(handles_[2], hashes_data_key.Encode(), value_buf);
      *ret = value;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
      s = db_->Get(default_read_options_, handles_[2], hashes_data_key.Encode(), &old_value);
      if (s.ok()) {
        int64_t ival = 0;
        if (StrToInt64(old_value.data(), old_value.size(), &ival) == 0) {
          return Status::Corruption("hash value is not an integer");
        }
        if ((value >= 0 && LLONG_MAX - value < ival) || (value < 0 && LLONG_MIN - value > ival)) {
          return Status::InvalidArgument("Overflow");
        }
        *ret = ival + value;
        Int64ToStr(value_buf, 32, *ret);
        batch.Put(handles_[2], hashes_data_key.Encode(), value_buf);
        statistic++;
      } else if (s.IsNotFound()) {
        Int64ToStr(value_buf, 32, value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[2], hashes_data_key.Encode(), value_buf);
        *ret = value;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(Slice(meta_value_buf, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[1], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);

    Int64ToStr(value_buf, 32, value);
    batch.Put(handles_[2], hashes_data_key.Encode(), value_buf);
    *ret = value;
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status Instance::HIncrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value) {
  new_value->clear();
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  std::string old_value_str;
  long double long_double_by;

  if (StrToLongDouble(by.data(), by.size(), &long_double_by) == -1) {
    return Status::Corruption("value is not a vaild float");
  }

  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.SetEtime(0);
      batch.Put(handles_[1], key, meta_value);
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);

      LongDoubleToStr(long_double_by, new_value);
      batch.Put(handles_[2], hashes_data_key.Encode(), *new_value);
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
      s = db_->Get(default_read_options_, handles_[2], hashes_data_key.Encode(), &old_value_str);
      if (s.ok()) {
        long double total;
        long double old_value;
        if (StrToLongDouble(old_value_str.data(), old_value_str.size(), &old_value) == -1) {
          return Status::Corruption("value is not a vaild float");
        }

        total = old_value + long_double_by;
        if (LongDoubleToStr(total, new_value) == -1) {
          return Status::InvalidArgument("Overflow");
        }
        batch.Put(handles_[2], hashes_data_key.Encode(), *new_value);
        statistic++;
      } else if (s.IsNotFound()) {
        LongDoubleToStr(long_double_by, new_value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[1], key, meta_value);
        batch.Put(handles_[2], hashes_data_key.Encode(), *new_value);
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(Slice(meta_value_buf, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[1], base_meta_key.Encode(), hashes_meta_value.Encode());

    HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
    LongDoubleToStr(long_double_by, new_value);
    batch.Put(handles_[2], hashes_data_key.Encode(), *new_value);
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status Instance::HKeys(const Slice& key, std::vector<std::string>* fields) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        fields->push_back(parsed_hashes_data_key.field().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status Instance::HLen(const Slice& key, int32_t* ret) {
  *ret = 0;
  std::string meta_value;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      *ret = parsed_hashes_meta_value.count();
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status Instance::HMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss) {
  vss->clear();

  int32_t version = 0;
  bool is_stale = false;
  std::string value;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if ((is_stale = parsed_hashes_meta_value.IsStale()) || parsed_hashes_meta_value.count() == 0) {
      for (size_t idx = 0; idx < fields.size(); ++idx) {
        vss->push_back({std::string(), Status::NotFound()});
      }
      return Status::NotFound(is_stale ? "Stale" : "");
    } else {
      version = parsed_hashes_meta_value.version();
      for (const auto& field : fields) {
        HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
        s = db_->Get(read_options, handles_[2], hashes_data_key.Encode(), &value);
        if (s.ok()) {
          vss->push_back({value, Status::OK()});
        } else if (s.IsNotFound()) {
          vss->push_back({std::string(), Status::NotFound()});
        } else {
          vss->clear();
          return s;
        }
      }
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    for (size_t idx = 0; idx < fields.size(); ++idx) {
      vss->push_back({std::string(), Status::NotFound()});
    }
  }
  return s;
}

Status Instance::HMSet(const Slice& key, const std::vector<FieldValue>& fvs) {
  uint32_t statistic = 0;
  std::unordered_set<std::string> fields;
  std::vector<FieldValue> filtered_fvs;
  for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
    std::string field = iter->field;
    if (fields.find(field) == fields.end()) {
      fields.insert(field);
      filtered_fvs.push_back(*iter);
    }
  }

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string meta_value;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.set_count(static_cast<int32_t>(filtered_fvs.size()));
      batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, fv.field);
        batch.Put(handles_[2], hashes_data_key.Encode(), fv.value);
      }
    } else {
      int32_t count = 0;
      std::string data_value;
      version = parsed_hashes_meta_value.version();
      for (const auto& fv : filtered_fvs) {
        HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, fv.field);
        s = db_->Get(default_read_options_, handles_[2], hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          statistic++;
          batch.Put(handles_[2], hashes_data_key.Encode(), fv.value);
        } else if (s.IsNotFound()) {
          count++;
          batch.Put(handles_[2], hashes_data_key.Encode(), fv.value);
        } else {
          return s;
        }
      }
      parsed_hashes_meta_value.ModifyCount(count);
      batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, filtered_fvs.size());
    HashesMetaValue hashes_meta_value(Slice(meta_value_buf, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[1], base_meta_key.Encode(), hashes_meta_value.Encode());
    for (const auto& fv : filtered_fvs) {
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, fv.field);
      batch.Put(handles_[2], hashes_data_key.Encode(), fv.value);
    }
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status Instance::HSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.set_count(1);
      batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
      HashesDataKey data_key(0/*db_id*/, slot_id, key, version, field);
      batch.Put(handles_[2], data_key.Encode(), value);
      *res = 1;
    } else {
      version = parsed_hashes_meta_value.version();
      std::string data_value;
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
      s = db_->Get(default_read_options_, handles_[2], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *res = 0;
        if (data_value == value.ToString()) {
          return Status::OK();
        } else {
          batch.Put(handles_[2], hashes_data_key.Encode(), value);
          statistic++;
        }
      } else if (s.IsNotFound()) {
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[2], hashes_data_key.Encode(), value);
        *res = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue meta_value(Slice(meta_value_buf, sizeof(int32_t)));
    version = meta_value.UpdateVersion();
    batch.Put(handles_[1], base_meta_key.Encode(), meta_value.Encode());
    HashesDataKey data_key(0/*db_id*/, slot_id, key, version, field);
    batch.Put(handles_[2], data_key.Encode(), value);
    *res = 1;
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status Instance::HSetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string meta_value;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  char meta_value_buf[4] = {0};
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.set_count(1);
      batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
      batch.Put(handles_[2], hashes_data_key.Encode(), value);
      *ret = 1;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
      std::string data_value;
      s = db_->Get(default_read_options_, handles_[2], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *ret = 0;
      } else if (s.IsNotFound()) {
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[1], base_meta_key.Encode(), meta_value);
        batch.Put(handles_[2], hashes_data_key.Encode(), value);
        *ret = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    EncodeFixed32(meta_value_buf, 1);
    HashesMetaValue hashes_meta_value(Slice(meta_value_buf, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[1], base_meta_key.Encode(), hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, field);
    batch.Put(handles_[2], hashes_data_key.Encode(), value);
    *ret = 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status Instance::HVals(const Slice& key, std::vector<std::string>* values) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(0/*db_id*/, slot_id, key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
        values->push_back(iter->value().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status Instance::HStrlen(const Slice& key, const Slice& field, int32_t* len) {
  std::string value;
  Status s = HGet(key, field, &value);
  if (s.ok()) {
    *len = static_cast<int32_t>(value.size());
  } else {
    *len = 0;
  }
  return s;
}

Status Instance::HScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
                       std::vector<FieldValue>* field_values, int64_t* next_cursor) {
  *next_cursor = 0;
  field_values->clear();
  if (cursor < 0) {
    *next_cursor = 0;
    return Status::OK();
  }

  int64_t rest = count;
  int64_t step_length = count;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_field;
      std::string start_point;
      int32_t version = parsed_hashes_meta_value.version();
      s = GetScanStartPoint(DataType::kHashes, key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_field = pattern.substr(0, pattern.size() - 1);
      }

      HashesDataKey hashes_data_prefix(0/*db_id*/, slot_id, key, version, sub_field);
      HashesDataKey hashes_start_data_key(0/*db_id*/, slot_id, key, version, start_point);
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(hashes_start_data_key.Encode()); iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          field_values->push_back({field, iter->value().ToString()});
        }
        rest--;
      }

      if (iter->Valid() && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string next_field = parsed_hashes_data_key.field().ToString();
        StoreScanNextPoint(DataType::kHashes, key, pattern, *next_cursor, next_field);
      } else {
        *next_cursor = 0;
      }
      delete iter;
    }
  } else {
    *next_cursor = 0;
    return s;
  }
  return Status::OK();
}

Status Instance::HScanx(const Slice& key, const std::string& start_field, const std::string& pattern, int64_t count,
                           std::vector<FieldValue>* field_values, std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t rest = count;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      *next_field = "";
      return Status::NotFound();
    } else {
      int32_t version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_prefix(0/*db_id*/, slot_id, key, version, Slice());
      HashesDataKey hashes_start_data_key(0/*db_id*/, slot_id, key, version, start_field);
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(hashes_start_data_key.Encode()); iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          field_values->push_back({field, iter->value().ToString()});
        }
        rest--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        *next_field = parsed_hashes_data_key.field().ToString();
      } else {
        *next_field = "";
      }
      delete iter;
    }
  } else {
    *next_field = "";
    return s;
  }
  return Status::OK();
}

Status Instance::PKHScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                                 const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                                 std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t remain = limit;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool start_no_limit = field_start.compare("") == 0;
  bool end_no_limit = field_end.empty();

  if (!start_no_limit && !end_no_limit && (field_start.compare(field_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_prefix(0/*db_id*/, slot_id, key, version, Slice());
      HashesDataKey hashes_start_data_key(0/*db_id*/, slot_id, key, version, field_start);
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->Seek(start_no_limit ? prefix : hashes_start_data_key.Encode());
           iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (!end_no_limit && field.compare(field_end) > 0) {
          break;
        }
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          field_values->push_back({field, iter->value().ToString()});
        }
        remain--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        if (end_no_limit || parsed_hashes_data_key.field().compare(field_end) <= 0) {
          *next_field = parsed_hashes_data_key.field().ToString();
        }
      }
      delete iter;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status Instance::PKHRScanRange(const Slice& key, const Slice& field_start, const std::string& field_end,
                                  const Slice& pattern, int32_t limit, std::vector<FieldValue>* field_values,
                                  std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t remain = limit;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool start_no_limit = field_start.compare("") == 0;
  bool end_no_limit = field_end.empty();

  if (!start_no_limit && !end_no_limit && (field_start.compare(field_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(read_options, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale() || parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_hashes_meta_value.version();
      int32_t start_key_version = start_no_limit ? version + 1 : version;
      std::string start_key_field = start_no_limit ? "" : field_start.ToString();
      HashesDataKey hashes_data_prefix(0/*db_id*/, slot_id, key, version, Slice());
      HashesDataKey hashes_start_data_key(0/*db_id*/, slot_id, key, start_key_version, start_key_field);
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[2]);
      for (iter->SeekForPrev(hashes_start_data_key.Encode().ToString());
           iter->Valid() && remain > 0 && iter->key().starts_with(prefix); iter->Prev()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        std::string field = parsed_hashes_data_key.field().ToString();
        if (!end_no_limit && field.compare(field_end) < 0) {
          break;
        }
        if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0) != 0) {
          field_values->push_back({field, iter->value().ToString()});
        }
        remain--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key());
        if (end_no_limit || parsed_hashes_data_key.field().compare(field_end) >= 0) {
          *next_field = parsed_hashes_data_key.field().ToString();
        }
      }
      delete iter;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status Instance::HashesExpire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    }

    if (ttl > 0) {
      parsed_hashes_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[1], base_meta_key.Encode(), meta_value);
    } else {
      parsed_hashes_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[1], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

Status Instance::HashesDel(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = parsed_hashes_meta_value.count();
      parsed_hashes_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[1], base_meta_key.Encode(), meta_value);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
    }
  }
  return s;
}

Status Instance::HashesExpireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      if (timestamp > 0) {
        parsed_hashes_meta_value.SetEtime(uint64_t(timestamp));
      } else {
        parsed_hashes_meta_value.InitialMetaValue();
      }
      s = db_->Put(default_write_options_, handles_[1], base_meta_key.Encode(), meta_value);
    }
  }
  return s;
}

Status Instance::HashesPersist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t timestamp = parsed_hashes_meta_value.etime();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_hashes_meta_value.SetEtime(0);
        s = db_->Put(default_write_options_, handles_[1], base_meta_key.Encode(), meta_value);
      }
    }
  }
  return s;
}

Status Instance::HashesTTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  uint16_t slot_id = static_cast<uint16_t>(GetSlotID(key.ToString()));
  BaseMetaKey base_meta_key(0/*db_id*/, slot_id, key);
  Status s = db_->Get(default_read_options_, handles_[1], base_meta_key.Encode(), &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      *timestamp = parsed_hashes_meta_value.etime();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime >= 0 ? *timestamp - curtime : -2;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

void Instance::ScanHashes() {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  auto current_time = static_cast<int32_t>(time(nullptr));

  LOG(INFO) << "***************" << "rocksdb instance: " << index_ << " Hashes Meta Data***************";
  auto meta_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_hashes_meta_value.etime() != 0) {
      survival_time = parsed_hashes_meta_value.etime() - current_time > 0
                          ? parsed_hashes_meta_value.etime() - current_time
                          : -1;
    }
    ParsedBaseMetaKey parsed_meta_key(meta_iter->key());

    LOG(INFO) << fmt::format("[key : {:<30}] [count : {:<10}] [timestamp : {:<10}] [version : {}] [survival_time : {}]",
                             parsed_meta_key.Key().ToString(), parsed_hashes_meta_value.count(),
                             parsed_hashes_meta_value.etime(), parsed_hashes_meta_value.version(), survival_time);
  }
  delete meta_iter;

  LOG(INFO) << "***************Hashes Field Data***************";
  auto field_iter = db_->NewIterator(iterator_options, handles_[2]);
  for (field_iter->SeekToFirst(); field_iter->Valid(); field_iter->Next()) {
    ParsedHashesDataKey parsed_hashes_data_key(field_iter->key());

    LOG(INFO) << fmt::format("[key : {:<30}] [field : {:<20}] [value : {:<20}] [version : {}]",
                             parsed_hashes_data_key.key().ToString(), parsed_hashes_data_key.field().ToString(),
                             field_iter->value().ToString(), parsed_hashes_data_key.version());
  }
  delete field_iter;
}

}  //  namespace storage
