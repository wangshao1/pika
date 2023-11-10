//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_CUSTOM_COMPARATOR_H_
#define INCLUDE_CUSTOM_COMPARATOR_H_
#include "string"

#include <glog/logging.h>

#include "src/debug.h"
#include "src/coding.h"
#include "rocksdb/comparator.h"

namespace storage {
/* list data key pattern
* | reserve1 | db_id | slot_id | key size | key | version | index | reserve2 |
* |    8B    |   2B  |    2B   |    4B    |     |    8B   |  8B   |   16B    |
*/
class ListsDataKeyComparatorImpl : public rocksdb::Comparator {
 public:
  ListsDataKeyComparatorImpl() = default;

  // keep compatible with floyd
  const char* Name() const override { return "floyd.ListsDataKeyComparator"; }

  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
    assert(!a.empty() && !b.empty());
    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    auto a_size = static_cast<int32_t>(a.size());
    auto b_size = static_cast<int32_t>(b.size());
    uint32_t key_a_len = DecodeFixed32(ptr_a + sizeof(uint64_t) + 2 * sizeof(uint16_t));
    uint32_t key_b_len = DecodeFixed32(ptr_b + sizeof(uint64_t) + 2 * sizeof(uint16_t));
    ptr_a += sizeof(uint64_t) + 2 * sizeof(uint16_t) + sizeof(uint32_t);
    ptr_b += sizeof(uint64_t) + 2 * sizeof(uint16_t) + sizeof(uint32_t);
    rocksdb::Slice sets_key_a(ptr_a, key_a_len);
    rocksdb::Slice sets_key_b(ptr_b, key_b_len);
    ptr_a += key_a_len;
    ptr_b += key_b_len;
    if (sets_key_a != sets_key_b) {
      return sets_key_a.compare(sets_key_b);
    }
    if (ptr_a - a.data() == a_size && ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    uint64_t version_a = DecodeFixed64(ptr_a);
    uint64_t version_b = DecodeFixed64(ptr_b);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (version_a != version_b) {
      return version_a < version_b ? -1 : 1;
    }
    if (ptr_a - a.data() == a_size && ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    uint64_t index_a = DecodeFixed64(ptr_a);
    uint64_t index_b = DecodeFixed64(ptr_b);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (index_a != index_b) {
      return index_a < index_b ? -1 : 1;
    } else {
      return 0;
    }
  }

  bool Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const override { return Compare(a, b) == 0; }

  void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

/* zset score key pattern
 *  |--------kPrefixMetaLength----------|
 *  |-----------kPrefixWithKeySizeLength---------------|
 *  | <Reserve 1> |  <DBID>  | <SlotID> |  <Key Size>  |      <Key>      |  <Version>  |  <Score>  | <Member> | <Reserve2> |
 *  |   8 Bytes   | 2 Bytes  | 2 Bytes  |   4 Bytes    |  Key Size Bytes |   8 Bytes   |  8 Bytes  |          |     16B    |
 */
class ZSetsScoreKeyComparatorImpl : public rocksdb::Comparator {
 public:
  // keep compatible with floyd
  const char* Name() const override { return "floyd.ZSetsScoreKeyComparator"; }
  const int kPrefixMetaLength = 8 + 2 * sizeof(uint16_t);
  const int kPrefixWithKeySizeLength = kPrefixMetaLength + sizeof(uint32_t);
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
    assert(a.size() > kPrefixWithKeySizeLength); 
    assert(b.size() > kPrefixWithKeySizeLength); 

    LOG(WARNING) << "ZSetsScoreKeyComparator*************************";
    LOG(WARNING) << "ZSetsScoreKeyComparator a: " << get_printable_key(a.ToString());
    LOG(WARNING) << "ZSetsScoreKeyComparator b: " << get_printable_key(b.ToString());

    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    auto a_size = static_cast<int32_t>(a.size());
    auto b_size = static_cast<int32_t>(b.size());
    int32_t key_a_len = DecodeFixed32(ptr_a + kPrefixMetaLength);
    int32_t key_b_len = DecodeFixed32(ptr_b + kPrefixMetaLength);

    size_t a_prefix_to_version_length = kPrefixWithKeySizeLength + key_a_len + sizeof(uint64_t);
    size_t a_prefix_to_score_length = a_prefix_to_version_length + sizeof(uint64_t); 
    size_t b_prefix_to_version_length = kPrefixWithKeySizeLength + key_b_len + sizeof(uint64_t);
    size_t b_prefix_to_score_length = b_prefix_to_version_length + sizeof(uint64_t); 

    rocksdb::Slice key_a_prefix(ptr_a, a_prefix_to_version_length); 
    rocksdb::Slice key_b_prefix(ptr_b, a_prefix_to_version_length); 
    ptr_a += a_prefix_to_version_length; 
    ptr_b += b_prefix_to_version_length;
    int ret = key_a_prefix.compare(key_b_prefix);
    LOG(WARNING) << "key_a_prefix: " << get_printable_key(key_a_prefix.ToString());
    LOG(WARNING) << "key_b_prefix: " << get_printable_key(key_b_prefix.ToString());
    LOG(WARNING) << " ret: " << ret;
    if (ret) {
      return ret;
    }

    uint64_t a_i = DecodeFixed64(ptr_a);
    uint64_t b_i = DecodeFixed64(ptr_b);
    const void* ptr_a_score = reinterpret_cast<const void*>(&a_i);
    const void* ptr_b_score = reinterpret_cast<const void*>(&b_i);
    double a_score = *reinterpret_cast<const double*>(ptr_a_score);
    double b_score = *reinterpret_cast<const double*>(ptr_b_score);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    LOG(WARNING) << "a score: " << a_score << " b score: " << b_score;
    if (a_score != b_score) {
      LOG(WARNING) << "a: " << get_printable_key(a.ToString());
      LOG(WARNING) << "b: " << get_printable_key(b.ToString());
      LOG(WARNING) << " a_score: " << a_score << " b_score: " << b_score;
      return a_score < b_score ? -1 : 1;
    }

    rocksdb::Slice key_a_member(ptr_a, a_size - a_prefix_to_score_length);
    rocksdb::Slice key_b_member(ptr_b, b_size - b_prefix_to_score_length);
    LOG(WARNING) << "a member: " << get_printable_key(key_a_member.ToString());
    LOG(WARNING) << "b member: " << get_printable_key(key_b_member.ToString());
    LOG(WARNING) << " cmp:" << key_a_member.compare(key_b_member); 
    return key_a_member.compare(key_b_member);
  }

  bool Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const override { return Compare(a, b) == 0; }

  void ParseAndPrintZSetsScoreKey(const std::string& from, const std::string& str) {
    const char* ptr = str.data();

    int32_t key_len = DecodeFixed32(ptr + sizeof(int32_t) + sizeof(uint64_t));
    size_t prefix_to_version_length = kPrefixWithKeySizeLength + key_len + sizeof(uint64_t);
    size_t prefix_to_score_length = prefix_to_version_length + sizeof(uint64_t); 
    ptr += kPrefixWithKeySizeLength; 

    std::string key(ptr, key_len);
    ptr += key_len;

    uint64_t version = DecodeFixed64(ptr);
    ptr += sizeof(uint64_t);

    uint64_t key_score_i = DecodeFixed64(ptr);
    const void* ptr_key_score = reinterpret_cast<const void*>(&key_score_i);
    double score = *reinterpret_cast<const double*>(ptr_key_score);
    ptr += sizeof(uint64_t);

    std::string member(ptr, str.size() - prefix_to_score_length - 2 * sizeof(uint64_t)); 
    LOG(INFO) << from.data() << ": total_len[" << str.size() << "], key_len[" << key_len << "], key[" << key.data() << "], "
              << "version[ " << version << "], score[" << score << "], member[" << member.data() << "]";
  }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  // TODO(wangshaoyi): need reformat, if pkey differs, why return limit directly?
  void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {
    return;
    assert(start->size() > kPrefixWithKeySizeLength); 
    assert(limit.size() > kPrefixWithKeySizeLength); 

    const char* ptr_start = start->data();
    const char* ptr_limit = limit.data();
    int32_t key_start_len = DecodeFixed32(ptr_start);
    int32_t key_limit_len = DecodeFixed32(ptr_limit);
    size_t start_prefix_to_version_length = kPrefixWithKeySizeLength + key_start_len + sizeof(uint64_t);
    size_t start_prefix_to_score_length = start_prefix_to_version_length + sizeof(uint64_t); 
    size_t limit_prefix_to_version_length = kPrefixWithKeySizeLength + key_start_len + sizeof(uint64_t);
    size_t limit_prefix_to_score_length = limit_prefix_to_version_length + sizeof(uint64_t); 

    rocksdb::Slice key_start_prefix(ptr_start, start_prefix_to_version_length); 
    rocksdb::Slice key_limit_prefix(ptr_limit, limit_prefix_to_version_length); 
    ptr_start += start_prefix_to_version_length; 
    ptr_limit += limit_prefix_to_version_length;
    if (key_start_prefix.compare(key_limit_prefix) != 0) {
      return;
    }

    uint64_t start_i = DecodeFixed64(ptr_start);
    uint64_t limit_i = DecodeFixed64(ptr_limit);
    const void* ptr_start_score = reinterpret_cast<const void*>(&start_i);
    const void* ptr_limit_score = reinterpret_cast<const void*>(&limit_i);
    double start_score = *reinterpret_cast<const double*>(ptr_start_score);
    double limit_score = *reinterpret_cast<const double*>(ptr_limit_score);
    ptr_start += sizeof(uint64_t);
    ptr_limit += sizeof(uint64_t);
    if (start_score < limit_score) {
      if (start_score + 1 < limit_score) {
        start->resize(start_prefix_to_version_length);
        start_score += 1;
        const void* addr_start_score = reinterpret_cast<const void*>(&start_score);
        char dst[sizeof(uint64_t)];
        EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_start_score));
        start->append(dst, sizeof(uint64_t));
      }
      return;
    }

    std::string key_start_member(ptr_start, start->size() - start_prefix_to_score_length); 
    std::string key_limit_member(ptr_limit, limit.size() - limit_prefix_to_score_length); 
    // Find length of common prefix
    size_t min_length = std::min(key_start_member.size(), key_limit_member.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) && (key_start_member[diff_index] == key_limit_member[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      auto key_start_member_byte = static_cast<uint8_t>(key_start_member[diff_index]);
      auto key_limit_member_byte = static_cast<uint8_t>(key_limit_member[diff_index]);
      if (key_start_member_byte >= key_limit_member_byte) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        return;
      }
      assert(key_start_member_byte < key_limit_member_byte);

      if (diff_index < key_limit_member.size() - 1 || key_start_member_byte + 1 < key_limit_member_byte) {
        key_start_member[diff_index]++;
        key_start_member.resize(diff_index + 1);
        start->resize(start_prefix_to_score_length);
        start->append(key_start_member);
      } else {
        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diff_index++;

        while (diff_index < key_start_member.size()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if (static_cast<uint8_t>(key_start_member[diff_index]) < static_cast<uint8_t>(0xff)) {
            key_start_member[diff_index]++;
            key_start_member.resize(diff_index + 1);
            start->resize(start_prefix_to_score_length);
            start->append(key_start_member);
            break;
          }
          diff_index++;
        }
      }
    }
  }

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortSuccessor(std::string* key) const override {}
};

}  //  namespace storage
#endif  //  INCLUDE_CUSTOM_COMPARATOR_H_
