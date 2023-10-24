//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_META_VALUE_FORMAT_H_
#define SRC_LISTS_META_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"

namespace storage {

const uint64_t InitalLeftIndex = 9223372036854775807;
const uint64_t InitalRightIndex = 9223372036854775808U;

/*
*| list_size | version | left index | right index | reserve |  cdate | timestamp |
*|     4B    |    8B   |     8B     |      8B     |   16B   |    8B  |     8B    |
*/
class ListsMetaValue : public InternalValue {
 public:
  explicit ListsMetaValue(const rocksdb::Slice& user_value)
      : InternalValue(user_value), left_index_(InitalLeftIndex), right_index_(InitalRightIndex) {}

  size_t AppendTimestampAndVersion() override {
    size_t usize = user_value_.size();
    char* dst = start_;
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed64(dst, version_);
    return usize + sizeof(int64_t);
  }

  virtual size_t AppendIndex() {
    char* dst = start_;
    dst += user_value_.size() + sizeof(uint64_t);
    EncodeFixed64(dst, left_index_);
    dst += sizeof(int64_t);
    EncodeFixed64(dst, right_index_);
    dst += sizeof(int64_t);
    memcpy(dst, reserve_, 2 * sizeof(uint64_t));
    dst += 2 * sizeof(uint64_t);
    EncodeFixed64(dst, ctime_);
    dst += sizeof(int64_t);
    EncodeFixed64(dst, etime_);
    return 6 * sizeof(int64_t);
  }

  static const size_t kDefaultValueSuffixLength = 7 * sizeof(int64_t);

  rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    size_t len = AppendTimestampAndVersion() + AppendIndex();
    return rocksdb::Slice(start_, len);
  }

  uint64_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<uint64_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<uint64_t>(unix_time);
    }
    return version_;
  }

  uint64_t left_index() { return left_index_; }

  void ModifyLeftIndex(uint64_t index) { left_index_ -= index; }

  uint64_t right_index() { return right_index_; }

  void ModifyRightIndex(uint64_t index) { right_index_ += index; }

 private:
  uint64_t left_index_ = 0;
  uint64_t right_index_ = 0;
};

class ParsedListsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedListsMetaValue(std::string* internal_value_str)
      : ParsedInternalValue(internal_value_str) {
    assert(internal_value_str->size() >= kListsMetaValueSuffixLength);
    if (internal_value_str->size() >= kListsMetaValueSuffixLength) {
      int offset = 0;
      user_value_ = rocksdb::Slice(internal_value_str->data(), internal_value_str->size() - kListsMetaValueSuffixLength);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(uint64_t);
      left_index_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(uint64_t);
      right_index_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(uint64_t);
      memcpy(reserve_, internal_value_str->data() + offset, 2 * sizeof(uint64_t));
      offset += 2 * sizeof(uint64_t);
      ctime_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(uint64_t);
      etime_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(uint64_t);
    }
    count_ = DecodeFixed64(internal_value_str->data());
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedListsMetaValue(const rocksdb::Slice& internal_value_slice)
      : ParsedInternalValue(internal_value_slice) {
    assert(internal_value_slice.size() >= kListsMetaValueSuffixLength);
    if (internal_value_slice.size() >= kListsMetaValueSuffixLength) {
      int offset = 0;
      user_value_ = rocksdb::Slice(internal_value_slice.data(), internal_value_slice.size() - kListsMetaValueSuffixLength);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
      left_index_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
      right_index_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
      memcpy(reserve_, internal_value_slice.data() + offset, 2 * sizeof(uint64_t));
      offset += 2 * sizeof(uint64_t);
      ctime_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
      etime_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
    }
    count_ = DecodeFixed64(internal_value_slice.data());
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kListsMetaValueSuffixLength, kListsMetaValueSuffixLength);
    }
  }

  void SetVersionToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength;
      EncodeFixed64(dst, version_);
    }
  }

  void SetIndexToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 6 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
      dst += sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

  static const size_t kListsMetaValueSuffixLength = 7 * sizeof(int64_t);

  uint64_t InitialMetaValue() {
    this->set_count(0);
    this->set_left_index(InitalLeftIndex);
    this->set_right_index(InitalRightIndex);
    this->SetEtime(0);
    this->set_ctime(0);
    return this->UpdateVersion();
  }

  bool IsValid() override {
    return !IsStale() && count() != 0;
  }

  uint64_t count() { return count_; }

  void set_count(uint64_t count) {
    count_ = count;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, count_);
    }
  }

  void ModifyCount(uint64_t delta) {
    count_ += delta;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, count_);
    }
  }

  uint64_t UpdateVersion() {
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

  uint64_t left_index() { return left_index_; }

  void set_left_index(uint64_t index) {
    left_index_ = index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
    }
  }

  void ModifyLeftIndex(uint64_t index) {
    left_index_ -= index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
    }
  }

  uint64_t right_index() { return right_index_; }

  void set_right_index(uint64_t index) {
    right_index_ = index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

  void ModifyRightIndex(uint64_t index) {
    right_index_ += index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

 private:
  uint64_t count_ = 0;
  uint64_t left_index_ = 0;
  uint64_t right_index_ = 0;
};

}  //  namespace storage
#endif  //  SRC_LISTS_META_VALUE_FORMAT_H_
