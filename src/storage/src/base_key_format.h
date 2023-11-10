//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_KEY_FORMAT_H_
#define SRC_BASE_KEY_FORMAT_H_

#include "pstd/include/pstd_coding.h"

namespace storage {
/*
* used for string data key or hash/zset/set/list's meta key. format:
* | reserve1 | db_id | slot_id | key | reserve2 |
* |    8B    |   2B  |    2B   |     |   16B    |
*/
class BaseKey {
 public:
  BaseKey(uint16_t db_id, uint16_t slot_id, const Slice& key)
      : db_id_(db_id), slot_id_(slot_id), key_(key) {}
  BaseKey(const Slice& key)
      : db_id_(0), slot_id_(0), key_(key) {}

  ~BaseKey() {
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
    dst += sizeof(reserve1_);
    // db_id: 2 byte
    pstd::EncodeFixed16(dst, db_id_);
    dst += sizeof(db_id_);
    // slot_id: 2 byte
    pstd::EncodeFixed16(dst, slot_id_);
    dst += sizeof(slot_id_);
    // key
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    // TODO(wangshaoyi): too much for reserve
    // reserve2: 16 byte
    memcpy(dst, reserve2_, sizeof(reserve2_));
    return Slice(start_, needed);
  }

 private:
  char* start_ = nullptr;
  char space_[200];
  char reserve1_[8] = {0};
  uint16_t slot_id_ = (uint16_t)(-1);
  uint16_t db_id_ = (uint16_t)(-1);
  Slice key_;
  char reserve2_[16] = {0};
};

class ParsedBaseKey {
 public:
  explicit ParsedBaseKey(const std::string* key) {
    const char* ptr = key->data();
    const char* end_ptr = key->data() + key->size();
    decode(ptr, end_ptr);
  }

  explicit ParsedBaseKey(const Slice& key) {
    const char* ptr = key.data();
    const char* end_ptr = key.data() + key.size();
    decode(ptr, end_ptr);
  }

  void decode(const char* ptr, const char* end_ptr) {
    // skip reserve1_
    ptr += sizeof(reserve1_);

    db_id_ = pstd::DecodeFixed16(ptr);
    ptr += sizeof(db_id_);
    slot_id_ = pstd::DecodeFixed16(ptr);
    ptr += sizeof(slot_id_);
    // reserve2_
    end_ptr -= 16;
    key_ = Slice(ptr, std::distance(ptr, end_ptr));
  }

  virtual ~ParsedBaseKey() = default;

  Slice key() { return key_; }

  uint16_t slot_id() { return slot_id_; }

  uint16_t db_id() { return db_id_; }

protected:
  Slice key_;
  char reserve1_[16] = {0};
  uint16_t slot_id_ = (uint16_t)(-1);
  uint16_t db_id_ = (uint16_t)(-1);
};

}  //  namespace storage
#endif  // SRC_BASE_KEY_FORMAT_H_
