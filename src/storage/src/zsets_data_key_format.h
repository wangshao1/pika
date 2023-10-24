//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_DATA_KEY_FORMAT_H_
#define SRC_ZSETS_DATA_KEY_FORMAT_H_

#include "pstd/include/pstd_coding.h"

namespace storage {

/* zset score to member data key format:
* | reserve1 | db_id | slot_id |  key size | key | version | score | member |  reserve2 |
* |    8B    |   2B  |    2B   |     4B    |     |    8B   |  8B   |        |    16B    |
 */
class ZSetsScoreKey {
 public:
  ZSetsScoreKey(uint16_t db_id, uint16_t slot_id, const Slice& key, uint64_t version,
                double score, const Slice& member)
      : db_id_(db_id), slot_id_(slot_id), key_(key), version_(version),
        score_(score), member_(member) {}
  // for test
  ZSetsScoreKey(const Slice& key, int32_t version,
                double score, const Slice& member)
      : db_id_(0), slot_id_(0), key_(key), version_(version),
        score_(score), member_(member) {}

  ~ZSetsScoreKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice Encode() {
    size_t meta_size = 8 + 2 * sizeof(uint16_t) + 2 * sizeof(uint64_t) + 16;
    size_t usize = key_.size() + sizeof(int32_t) + member_.size();
    size_t needed = meta_size + usize;
    char* dst = nullptr;
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
    // score
    const void* addr_score = reinterpret_cast<const void*>(&score_);
    EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_score));
    dst += sizeof(uint64_t);
    // member
    memcpy(dst, member_.data(), member_.size());
    dst += member_.size();
    // reserve2 16 byte
    memcpy(dst, reserve2_, 16);
    return Slice(start_, needed);
  }

 private:
  char* start_ = nullptr;
  char space_[256];
  char reserve1_[8];
  uint16_t db_id_ = -1;
  uint16_t slot_id_ = -1;
  Slice key_;
  uint64_t version_ = 0;
  double score_ = 0.0;
  Slice member_;
  char reserve2_[16];
};

class ParsedZSetsScoreKey {
 public:
  explicit ParsedZSetsScoreKey(const std::string* key) {
    const char* ptr = key->data();
    const char* end_ptr = key->data() + key->size();
    decode(ptr, end_ptr);
  }

  explicit ParsedZSetsScoreKey(const Slice& key) {
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

    db_id_ = pstd::DecodeFixed16(ptr);
    ptr += sizeof(uint16_t);
    slot_id_ = pstd::DecodeFixed16(ptr);
    ptr += sizeof(uint16_t);
    int32_t key_len = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    meta_key_prefix_ = Slice(start, std::distance(start, ptr));

    version_ = pstd::DecodeFixed64(ptr);
    ptr += sizeof(int64_t);
    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    score_ = *reinterpret_cast<const double*>(ptr_tmp);
    ptr += sizeof(uint64_t);
    // reserve2_
    end_ptr -= 16;
    member_ = Slice(ptr, std::distance(ptr, end_ptr));
  }

  Slice key() { return key_; }
  int32_t version() const { return version_; }
  double score() const { return score_; }
  Slice member() { return member_; }

 private:
  Slice key_;
  Slice meta_key_prefix_;
  uint64_t version_ = 0;
  uint16_t slot_id_ = -1;
  uint16_t db_id_ = -1;
  double score_ = 0.0;
  Slice member_;
};

}  // namespace storage
#endif  // SRC_ZSETS_DATA_KEY_FORMAT_H_
