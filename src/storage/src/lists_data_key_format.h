//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_DATA_KEY_FORMAT_H_
#define SRC_LISTS_DATA_KEY_FORMAT_H_
#include <string>

#include "pstd/include/pstd_coding.h"
using rocksdb::Slice;

namespace storage {
/*
* used for List data key. format:
* | reserve1 | db_id | slot_id | key_size | key | version | index | reserve2 |
* |    8B    |   2B  |    2B   |    4B    |     |    8B   |   8B  |   16B    |
*/
// TODO(wangshaoyi): merge with base_data_key_format.h
class ListsDataKey {
public:
  ListsDataKey(uint16_t db_id, uint16_t slot_id, const Slice& key,
               uint64_t version, uint64_t index)
      : key_(key), version_(version), slot_id_(slot_id),
        db_id_(db_id), index_(index) {}
  //for test only
  ListsDataKey(const Slice& key, int32_t version, uint64_t index)
      : key_(key), version_(version), slot_id_(0),
        db_id_(0), index_(index) {}

  ~ListsDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice Encode() {
    size_t meta_size = 8 + 2 * sizeof(uint16_t) + sizeof(uint64_t) + 16;
    size_t usize = key_.size() + sizeof(int32_t) + sizeof(uint64_t);
    size_t needed = meta_size + usize;
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
    // reserve1: 8 byte
    memcpy(dst, reserve1_, 8);
    dst += 8;
    // db_id: 2 byte
    pstd::EncodeFixed16(dst, db_id_);
    dst += sizeof(int16_t);
    // slot_id: 2 byte
    pstd::EncodeFixed16(dst, slot_id_);
    dst += sizeof(int16_t);
    // key_size: 4 byte
    pstd::EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    // key
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    // version 8 byte
    pstd::EncodeFixed64(dst, version_);
    dst += sizeof(uint64_t);
    // index
    pstd::EncodeFixed64(dst, index_);
    dst += sizeof(uint64_t);
    // TODO(wangshaoyi): too much for reserve
    // reserve2: 16 byte
    memcpy(dst, reserve2_, 16);
    return Slice(start_, needed);
  }

private:
  char* start_ = nullptr;
  char space_[256];
  char reserve1_[8];
  uint16_t db_id_;
  uint16_t slot_id_;
  Slice key_;
  uint64_t version_;
  uint64_t index_ = 0;
  char reserve2_[16];
};

class ParsedListsDataKey {
 public:
  explicit ParsedListsDataKey(const std::string* key) {
    const char* ptr = key->data();
    const char* end_ptr = key->data() + key->size();
    decode(ptr, end_ptr);
  }

  explicit ParsedListsDataKey(const Slice& key) {
    const char* ptr = key.data();
    const char* end_ptr = key.data() + key.size();
    decode(ptr, end_ptr);
  }

  std::string EncodedMetaKey() {
    return meta_key_prefix_.ToString();
  }

  void decode(const char* ptr, const char* end_ptr) {
    const char* start = ptr;
    // skip reserve1_
    ptr += 8;

    uint16_t db_id = pstd::DecodeFixed16(ptr);
    ptr += sizeof(uint16_t);
    uint16_t slot_id = pstd::DecodeFixed16(ptr);
    ptr += sizeof(uint16_t);
    int32_t key_len = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    meta_key_prefix_ = Slice(start, std::distance(start, ptr));
    version_ = pstd::DecodeFixed64(ptr);
    ptr += sizeof(int64_t);
    index_ = pstd::DecodeFixed64(ptr);
  }

  virtual ~ParsedListsDataKey() = default;

  Slice key() { return key_; }

  uint64_t version() { return version_; }

  uint16_t slot_id() { return slot_id_; }

  uint16_t db_id() { return db_id_; }

  uint64_t index() { return index_; }

 private:
  Slice key_;
  Slice meta_key_prefix_;
  uint16_t slot_id_ = -1;
  uint16_t db_id_ = -1;
  uint64_t version_ = -1;
  uint64_t index_ = 0;
};

}  //  namespace storage
#endif  // SRC_LISTS_DATA_KEY_FORMAT_H_
