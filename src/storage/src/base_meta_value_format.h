//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_META_VALUE_FORMAT_H_
#define SRC_BASE_META_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"

namespace storage {

/*
* | value | version | reserve | cdate | timestamp |
* |       |    8B   |   16B   |   8B  |     8B    |
*/
// TODO(wangshaoyi): reformat encode, AppendTimestampAndVersion
class BaseMetaValue : public InternalValue {
 public:
  explicit BaseMetaValue(const Slice& user_value) : InternalValue(user_value) {}
  size_t AppendTimestampAndVersion() override {
    size_t usize = user_value_.size();
    char* dst = start_;
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed64(dst, version_);
    dst += sizeof(int64_t);
    memcpy(dst, reserve_, 2 * sizeof(uint64_t));
    dst += 2 * sizeof(uint64_t);
    EncodeFixed64(dst, ctime_);
    dst += sizeof(int64_t);
    EncodeFixed64(dst, etime_);
    return usize + 5 * sizeof(int64_t);
  }

  uint64_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= uint64_t(unix_time)) {
      version_++;
    } else {
      version_ = uint64_t(unix_time);
    }
    return version_;
  }
};

class ParsedBaseMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedBaseMetaValue(std::string* internal_value_str) : ParsedInternalValue(internal_value_str) {
    if (internal_value_str->size() >= kBaseMetaValueSuffixLength) {
      int offset = 0;
      user_value_ = Slice(internal_value_str->data(), internal_value_str->size() - kBaseMetaValueSuffixLength);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(uint64_t);
      memcpy(reserve_, internal_value_str->data() + offset, 2 * sizeof(uint64_t));
      offset += 2 * sizeof(uint64_t);
      ctime_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(uint64_t);
      etime_ = DecodeFixed64(internal_value_str->data() + offset);
    }
    count_ = DecodeFixed32(internal_value_str->data());
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedBaseMetaValue(const Slice& internal_value_slice) : ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kBaseMetaValueSuffixLength) {
      int offset = 0;
      user_value_ = Slice(internal_value_slice.data(), internal_value_slice.size() - kBaseMetaValueSuffixLength);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
      memcpy(reserve_, internal_value_slice.data() + offset, 2 * sizeof(uint64_t));
      offset += 2 * sizeof(uint64_t);
      ctime_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
      etime_ = DecodeFixed64(internal_value_slice.data() + offset);
    }
    count_ = DecodeFixed32(internal_value_slice.data());
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kBaseMetaValueSuffixLength, kBaseMetaValueSuffixLength);
    }
  }

  void SetVersionToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kBaseMetaValueSuffixLength;
      EncodeFixed64(dst, version_);
    }
  }

  static const size_t kBaseMetaValueSuffixLength = 5 * sizeof(uint64_t);

  int32_t InitialMetaValue() {
    this->set_count(0);
    this->SetEtime(0);
    this->set_ctime(0);
    return this->UpdateVersion();
  }

  bool IsValid() override {
    return !IsStale() && count() != 0;
  }

  int32_t count() { return count_; }

  void set_count(int32_t count) {
    count_ = count;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed32(dst, count_);
    }
  }

  void ModifyCount(int32_t delta) {
    count_ += delta;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed32(dst, count_);
    }
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<uint64_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<uint64_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

 private:
  int32_t count_ = 0;
};

using HashesMetaValue = BaseMetaValue;
using ParsedHashesMetaValue = ParsedBaseMetaValue;
using SetsMetaValue = BaseMetaValue;
using ParsedSetsMetaValue = ParsedBaseMetaValue;
using ZSetsMetaValue = BaseMetaValue;
using ParsedZSetsMetaValue = ParsedBaseMetaValue;

}  //  namespace storage
#endif  // SRC_BASE_META_VALUE_FORMAT_H_
