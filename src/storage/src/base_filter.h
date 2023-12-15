//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_FILTER_H_
#define SRC_BASE_FILTER_H_

#include <memory>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "rocksdb/compaction_filter.h"
#include "src/base_data_key_format.h"
#include "src/base_meta_value_format.h"
#include "src/debug.h"
#include "src/instance.h"

namespace storage {

class BaseMetaFilter : public rocksdb::CompactionFilter {
 public:
  BaseMetaFilter() = default;
  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    auto cur_time = static_cast<int32_t>(unix_time);
    ParsedBaseMetaValue parsed_base_meta_value(value);
    TRACE("==========================START==========================");
    TRACE("[MetaFilter], key: %s, count = %d, timestamp: %d, cur_time: %d, version: %d", key.ToString().c_str(),
          parsed_base_meta_value.Count(), parsed_base_meta_value.timestamp(), cur_time,
          parsed_base_meta_value.Version());

    if (parsed_base_meta_value.Etime() != 0 && parsed_base_meta_value.Etime() < cur_time &&
        parsed_base_meta_value.Version() < cur_time) {
      TRACE("Drop[Stale & version < cur_time]");
      return true;
    }
    if (parsed_base_meta_value.Count() == 0 && parsed_base_meta_value.Version() < cur_time) {
      TRACE("Drop[Empty & version < cur_time]");
      return true;
    }
    TRACE("Reserve");
    return false;
  }

  const char* Name() const override { return "BaseMetaFilter"; }
};

class BaseMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseMetaFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new BaseMetaFilter());
  }
  const char* Name() const override { return "BaseMetaFilterFactory"; }
};

class BaseDataFilter : public rocksdb::CompactionFilter {
 public:
  BaseDataFilter(rocksdb::DB* db, std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr, int meta_cf_index)
      : db_(db),
        cf_handles_ptr_(cf_handles_ptr),
        meta_cf_index_(meta_cf_index)
        {}

  bool Filter(int level, const Slice& key, const rocksdb::Slice& value, std::string* new_value,
              bool* value_changed) const override {
    ParsedBaseDataKey parsed_base_data_key(key);
    TRACE("==========================START==========================");
    TRACE("[DataFilter], key: %s, data = %s, version = %d", parsed_base_data_key.Key().ToString().c_str(),
          parsed_base_data_key.data().ToString().c_str(), parsed_base_data_key.Version());

    ParsedBaseMetaKey pbmk(key); 
    if (pbmk.Key().ToString() != cur_key_) {
      cur_key_ = pbmk.Key().ToString();
      auto iter = mem_.find(cur_key_);
      if (iter == mem_.end()) {
        meta_not_found_ = true; 
      } else {
        cur_meta_version_ = iter->second.first;
        cur_meta_etime_ = iter->second.second;
      }
    }

    if (meta_not_found_) {
      TRACE("Drop[Meta key not exist]");
      return true;
    }

    LOG(WARNING) << "user_key: " << cur_key_ << " meta not found: " << meta_not_found_ << " meta_version: " << cur_meta_version_ << " meta_etime: " << cur_meta_etime_;

    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (cur_meta_etime_ != 0 && cur_meta_etime_ < static_cast<int32_t>(unix_time)) {
      TRACE("Drop[Timeout]");
      return true;
    }

    if (cur_meta_version_ > parsed_base_data_key.Version()) {
      TRACE("Drop[data_key_version < cur_meta_version]");
      return true;
    } else {
      TRACE("Reserve[data_key_version == cur_meta_version]");
      return false;
    }
  }

  const char* Name() const override { return "BaseDataFilter"; }

 private:
  rocksdb::DB* db_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_ = false;
  mutable uint64_t cur_meta_version_ = 0;
  mutable uint64_t cur_meta_etime_ = 0;
  int meta_cf_index_ = 0;
};

class BaseDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseDataFilterFactory(rocksdb::DB** db_ptr, std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr, int meta_cf_index)
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr), meta_cf_index_(meta_cf_index) {}
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new BaseDataFilter(*db_ptr_, cf_handles_ptr_, meta_cf_index_));
  }
  const char* Name() const override { return "BaseDataFilterFactory"; }

 private:
  rocksdb::DB** db_ptr_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_ = nullptr;
  int meta_cf_index_ = 0;
};

using HashesMetaFilter = BaseMetaFilter;
using HashesMetaFilterFactory = BaseMetaFilterFactory;
using HashesDataFilter = BaseDataFilter;
using HashesDataFilterFactory = BaseDataFilterFactory;

using SetsMetaFilter = BaseMetaFilter;
using SetsMetaFilterFactory = BaseMetaFilterFactory;
using SetsMemberFilter = BaseDataFilter;
using SetsMemberFilterFactory = BaseDataFilterFactory;

using ZSetsMetaFilter = BaseMetaFilter;
using ZSetsMetaFilterFactory = BaseMetaFilterFactory;
using ZSetsDataFilter = BaseDataFilter;
using ZSetsDataFilterFactory = BaseDataFilterFactory;

}  //  namespace storage
#endif  // SRC_BASE_FILTER_H_
