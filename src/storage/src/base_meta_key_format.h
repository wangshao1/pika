//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_META_KEY_FORMAT_H_
#define SRC_BASE_META_KEY_FORMAT_H_

#include "rocksdb/slice.h"
#include "pstd/include/pstd_coding.h"

namespace storage {

using Slice = rocksdb::Slice;
/*
* used for Hash/Set/Zset meta key and string key. format:
* | reserve1 | db_id | slot_id | key | reserve2 |
* |    8B    |   2B  |    2B   |     |    16B   |
*/
class BaseMetaKey {
 public:
  BaseMetaKey(uint16_t db_id, uint16_t slot_id, const Slice& key)
      : key_(key), slot_id_(slot_id) {}

  BaseMetaKey(const Slice& key) : db_id_(0), slot_id_(0), key_(key) {}

  ~BaseMetaKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice Encode() {
    size_t meta_size = sizeof(reserve1_) + sizeof(db_id_) + sizeof(slot_id_) + sizeof(reserve2_);
    size_t usize = key_.size();
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
    memcpy(dst, reserve1_, sizeof(reserve1_));
    dst += 8;
    // db_id: 2 byte
    pstd::EncodeFixed16(dst, db_id_);
    dst += sizeof(db_id_);
    // slot_id: 2 byte
    pstd::EncodeFixed16(dst, slot_id_);
    dst += sizeof(slot_id_);
    // key
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    memcpy(dst, reserve2_, sizeof(reserve2_));
    return Slice(start_, needed);
  }

 private:
  char* start_ = nullptr;
  char space_[200];
  char reserve1_[8] = {0};
  uint16_t db_id_;
  uint16_t slot_id_;
  Slice key_;
  char reserve2_[16] = {0};
};

class ParsedBaseMetaKey {
 public:
  explicit ParsedBaseMetaKey(const std::string* key) {
    const char* ptr = key->data();
    const char* end_ptr = key->data() + key->size();
    decode(ptr, end_ptr);
  }

  explicit ParsedBaseMetaKey(const Slice& key) {
    const char* ptr = key.data();
    const char* end_ptr = key.data() + key.size();
    decode(ptr, end_ptr);
  }

  virtual ~ParsedBaseMetaKey() = default;

  void decode(const char* ptr, const char* end_ptr) {
    const char* start = ptr;
    // skip reserve1_
    ptr += sizeof(reserve1_);

    db_id_ = pstd::DecodeFixed16(ptr);
    ptr += sizeof(uint16_t);
    slot_id_ = pstd::DecodeFixed16(ptr);
    ptr += sizeof(uint16_t);

    // skip reserve2_
    end_ptr -= sizeof(reserve2_);

    key_ = Slice(ptr, std::distance(ptr, end_ptr));
  }

  Slice Key() { return key_; }

  uint16_t SlotID() { return slot_id_; }

  uint16_t DBID() { return db_id_; }

 protected:
  char reserve1_[8] = {0};
  Slice key_;
  uint16_t slot_id_ = uint16_t(-1);
  uint16_t db_id_ = uint16_t(-1);
  char reserve2_[16] = {0};
};
}  //  namespace storage
#endif  // SRC_BASE_DATA_KEY_FORMAT_H_
