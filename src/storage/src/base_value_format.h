//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_VALUE_FORMAT_H_
#define SRC_BASE_VALUE_FORMAT_H_

#include <string>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"

#include "src/coding.h"
#include "src/mutex.h"
#include "pstd/include/pstd_coding.h"

namespace storage {
class InternalValue {
public:
  explicit InternalValue(const rocksdb::Slice& user_value)
      :  user_value_(user_value) {}
  virtual ~InternalValue() {
    if (start_ != space_) {
      delete[] start_;
    }
  }
  void SetEtime(uint64_t etime = 0) { etime_ = etime; }
  void setCtime(uint64_t ctime) { ctime_ = ctime; }
  rocksdb::Status SetRelativeTimestamp(uint64_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    etime_ = uint64_t(unix_time) + ttl;
    if (etime_ != uint64_t(unix_time) + ttl) {
      return rocksdb::Status::InvalidArgument("invalid expire time");
    }
    return rocksdb::Status::OK();
  }
  void SetVersion(uint64_t version = 0) { version_ = version; }
  virtual rocksdb::Slice Encode() {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];

      // Need to allocate space, delete previous space
      if (start_ != space_) {
        delete[] start_;
      }
    }
    start_ = dst;
    size_t len = AppendTimestampAndVersion();
    return rocksdb::Slice(start_, len);
  }
  virtual size_t AppendTimestampAndVersion() {
    char* dst = start_;
    memcpy(dst, user_value_.data(), user_value_.size());
    dst += user_value_.size();
    memcpy(dst, reserve_, sizeof(reserve_));
    dst += sizeof(reserve_);
    EncodeFixed64(dst, ctime_);
    dst += sizeof(ctime_);
    return std::distance(start_, dst);
  }

protected:
  char space_[200];
  char* start_ = nullptr;
  rocksdb::Slice user_value_;
  uint64_t version_ = 0;
  uint64_t etime_ = 0;
  uint64_t ctime_ = 0;
  char reserve_[16] = {0};

private:
  const size_t kDefaultValueSuffixLength = sizeof(reserve_) + sizeof(ctime_);
};

class ParsedInternalValue {
public:
  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedInternalValue(std::string* value) : value_(value) {
    if (value_->size() >= kDefaultInternalValueSuffixLength) {
      user_value_ = rocksdb::Slice(value_->data(), value_->size() - kDefaultInternalValueSuffixLength);
      memcpy(reserve_, value_->data() + user_value_.size(), sizeof(reserve_));
      ctime_ = DecodeFixed64(value_->data() + user_value_.size() + sizeof(reserve_));
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the rocksdb::Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedInternalValue(const rocksdb::Slice& value)  {
    if (value.size() >= kDefaultInternalValueSuffixLength) {
      user_value_ = rocksdb::Slice(value.data(), value.size() - kDefaultInternalValueSuffixLength);
      memcpy(reserve_, value.data() + user_value_.size(), sizeof(reserve_));
      ctime_ = DecodeFixed64(value.data() + user_value_.size() + sizeof(reserve_));
    }
  }

  virtual ~ParsedInternalValue() = default;

  rocksdb::Slice UserValue() { return user_value_; }

  uint64_t version() { return version_; }

  void set_version(uint64_t version) {
    version_ = version;
    SetVersionToValue();
  }

  uint64_t etime() { return etime_; }

  uint64_t timestamp() { return etime_; }

  void SetEtime(uint64_t etime) {
    etime_ = etime;
    SetEtimeToValue();
  }

  void SetCtime(uint64_t ctime) {
    ctime_ = ctime;
    SetCtimeToValue();
  }

  void SetRelativeTimestamp(uint64_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    etime_ = static_cast<uint64_t>(unix_time) + ttl;
    SetEtimeToValue();
  }

  bool IsPermanentSurvival() { return etime_ == 0; }

  bool IsStale() {
    if (etime_ == 0) {
      return false;
    }
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    return etime_ < unix_time;
  }

  virtual bool IsValid() {
    return !IsStale();
  }

  void SetEtimeToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(uint64_t);
      EncodeFixed64(dst, etime_);
    }
  }

  void SetCtimeToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * sizeof(uint64_t);
      EncodeFixed64(dst, ctime_);
    }
  }

  void SetReserveToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 4 * sizeof(uint64_t);
      memcpy(dst, reserve_, 2 * sizeof(uint64_t));
    }
  }

  virtual void StripSuffix() {
    if (value_) {
      value_->erase(value_->size() - kDefaultInternalValueSuffixLength, kDefaultInternalValueSuffixLength);
    }
  }

protected:
  virtual void SetVersionToValue() {};
  std::string* value_ = nullptr;
  rocksdb::Slice user_value_;
  uint64_t version_ = 0 ;
  uint64_t ctime_ = 0;
  uint64_t etime_ = 0;
  char reserve_[16] = {0}; //unused

private:
  const size_t kDefaultInternalValueSuffixLength = sizeof(reserve_) + sizeof(ctime_);
};

}  //  namespace storage
#endif  // SRC_BASE_VALUE_FORMAT_H_
