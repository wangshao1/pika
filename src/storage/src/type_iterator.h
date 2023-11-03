#ifndef TYPE_ITERATOR_H_
#define TYPE_ITERATOR_H_

#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

#include "pstd/include/heap.h"
#include "storage/util.h"
#include "src/mutex.h"
#include "src/base_data_key_format.h"
#include "src/base_meta_key_format.h"
#include "src/base_meta_value_format.h"
#include "src/strings_value_format.h"
#include "src/lists_meta_value_format.h"

namespace storage {
using ColumnFamilyHandle = rocksdb::ColumnFamilyHandle;
using Comparator = rocksdb::Comparator;

class TypeIterator {
public:
  TypeIterator(const rocksdb::ReadOptions& options, rocksdb::DB* db,
  ColumnFamilyHandle* handle) {
    raw_iter_ = db->NewIterator(options, handle);
    cmp_ = handle->GetComparator();
  }

  virtual ~TypeIterator() { delete raw_iter_; }

  virtual void Seek(const std::string& start_key) {
    raw_iter_->Seek(Slice(start_key));
    while (raw_iter_->Valid() && ShouldSkip()) {
      raw_iter_->Next();
    }
  }

  void SeekToFirst() {
    raw_iter_->SeekToFirst();
    while (raw_iter_->Valid() && ShouldSkip()) {
      raw_iter_->Next();
    }
  }

  void SeekToLast() {
    raw_iter_->SeekToLast();
    while (raw_iter_->Valid() && ShouldSkip()) {
      raw_iter_->Prev();
    }
  }

  virtual void SeekForPrev(const std::string& start_key) {
    raw_iter_->SeekForPrev(Slice(start_key));
    while (raw_iter_->Valid() && ShouldSkip()) {
      raw_iter_->Prev();
    }
  }

  void Next() {
    raw_iter_->Next();
    while (raw_iter_->Valid() && ShouldSkip()) {
      raw_iter_->Next();
    }
  }

  void Prev() {
    raw_iter_->Prev();
    while (raw_iter_->Valid() && ShouldSkip()) {
      raw_iter_->Prev();
    }
  }

  virtual bool ShouldSkip() { return false; }

  const Comparator* GetComparator() { return cmp_;}

  virtual std::string Key() const { return user_key_; }

  virtual std::string Value() const {return user_value_; }

  virtual bool Valid() { return raw_iter_->Valid(); }

  virtual Status status() { return raw_iter_->status(); }

protected:
  rocksdb::Iterator* raw_iter_;
  const Comparator* cmp_;
  std::string user_key_;
  std::string user_value_;
};

class StringsIterator : public TypeIterator {
public:
  StringsIterator(const rocksdb::ReadOptions& options, rocksdb::DB* db,
                  ColumnFamilyHandle* handle,
                  const std::string& pattern)
      : TypeIterator(options, db, handle), pattern_(pattern) {}
  ~StringsIterator() {}

  bool ShouldSkip() override {
    ParsedStringsValue parsed_value(raw_iter_->value());
    if (parsed_value.IsStale()) {
      return true;
    }

    ParsedBaseMetaKey parsed_key(raw_iter_->key().ToString());
    if (StringMatch(pattern_.data(), pattern_.size(),
        parsed_key.Key().data(), parsed_key.Key().size(), 0) == 0) {
      return true;
    }
    user_key_ = parsed_key.Key().ToString();
    user_value_ = parsed_value.UserValue().ToString();
    return false;
  }
private:
  std::string pattern_;

};

class HashesIterator : public TypeIterator {
public:
  HashesIterator(const rocksdb::ReadOptions& options, rocksdb::DB* db,
                 ColumnFamilyHandle* handle,
                 const std::string& pattern)
      : TypeIterator(options, db, handle), pattern_(pattern) {}
  ~HashesIterator() {}

  bool ShouldSkip() override {
    ParsedHashesMetaValue parsed_meta_value(raw_iter_->value());
    if (parsed_meta_value.IsStale() || parsed_meta_value.count() == 0) {
      return true;
    }

    ParsedBaseMetaKey parsed_key(raw_iter_->key().ToString());
    if (StringMatch(pattern_.data(), pattern_.size(),
                    parsed_key.Key().data(), parsed_key.Key().size(), 0) == 0) {
      return true;
    }
    user_key_ = parsed_key.Key().ToString();
    user_value_ = parsed_meta_value.UserValue().ToString();
    return false;
  }
private:
  std::string pattern_;
};

class ListsIterator : public TypeIterator {
public:
  ListsIterator(const rocksdb::ReadOptions& options, rocksdb::DB* db,
                ColumnFamilyHandle* handle,
                const std::string& pattern)
      : TypeIterator(options, db, handle), pattern_(pattern) {}
  ~ListsIterator() {}

  bool ShouldSkip() override {
    ParsedListsMetaValue parsed_meta_value(raw_iter_->value());
    if (parsed_meta_value.IsStale() || parsed_meta_value.count() == 0) {
      return true;
    }

    ParsedBaseMetaKey parsed_key(raw_iter_->key().ToString());
    if (StringMatch(pattern_.data(), pattern_.size(),
        parsed_key.Key().data(), parsed_key.Key().size(), 0) == 0) {
      return true;
    }
    user_key_ = parsed_key.Key().ToString();
    user_value_ = parsed_meta_value.UserValue().ToString();
    return false;
  }
private:
  std::string pattern_;
};

class SetsIterator : public TypeIterator {
public:
  SetsIterator(const rocksdb::ReadOptions& options, rocksdb::DB* db,
               ColumnFamilyHandle* handle,
               const std::string& pattern)
      : TypeIterator(options, db, handle), pattern_(pattern) {}
  ~SetsIterator() {}

  bool ShouldSkip() override {
    ParsedSetsMetaValue parsed_meta_value(raw_iter_->value());
    if (parsed_meta_value.IsStale() || parsed_meta_value.count() == 0) {
      return true;
    }

    ParsedBaseMetaKey parsed_key(raw_iter_->key().ToString());
    if (StringMatch(pattern_.data(), pattern_.size(),
        parsed_key.Key().data(), parsed_key.Key().size(), 0) == 0) {
      return true;
    }
    user_key_ = parsed_key.Key().ToString();
    user_value_ = parsed_meta_value.UserValue().ToString();
    return false;
  }
private:
  std::string pattern_;
};

class ZsetsIterator : public TypeIterator {
public:
  ZsetsIterator(const rocksdb::ReadOptions& options, rocksdb::DB* db,
                ColumnFamilyHandle* handle,
                const std::string& pattern)
      : TypeIterator(options, db, handle), pattern_(pattern) {}
  ~ZsetsIterator() {}

  bool ShouldSkip() override {
    ParsedZSetsMetaValue parsed_meta_value(raw_iter_->value());
    if (parsed_meta_value.IsStale() || parsed_meta_value.count() == 0) {
      return true;
    }

    ParsedBaseMetaKey parsed_key(raw_iter_->key().ToString());
    if (StringMatch(pattern_.data(), pattern_.size(),
        parsed_key.Key().data(), parsed_key.Key().size(), 0) == 0) {
      return true;
    }
    user_key_ = parsed_key.Key().ToString();
    user_value_ = parsed_meta_value.UserValue().ToString();
    return false;
  }
private:
  std::string pattern_;
};

class MinMergeComparator {
public:
  MinMergeComparator(const Comparator* cmp) : cmp_(cmp) {}
  bool operator() (TypeIterator* a, TypeIterator* b) {
    return cmp_->Compare(a->Key(), b->Key()) > 0;
  }
private:
  const Comparator* cmp_;
};

class MaxMergeComparator {
public:
  MaxMergeComparator(const Comparator* cmp) : cmp_(cmp) {}
  bool operator() (TypeIterator* a, TypeIterator* b) {
    return cmp_->Compare(a->Key(), b->Key()) < 0;
  }
private:
  const Comparator* cmp_;
};

using MergerMinIterHeap = pstd::BinaryHeap<TypeIterator*, MinMergeComparator>;
using MergerMaxIterHeap = pstd::BinaryHeap<TypeIterator*, MaxMergeComparator>;

class MergingIterator {
public:
  MergingIterator(std::vector<TypeIterator*> children)
      :  current_(nullptr), min_heap_(children[0]->GetComparator()),
         max_heap_(children[0]->GetComparator()),
         direction_(kForward) {
    std::copy(children.begin(), children.end(), std::back_inserter(children_));
    for (const auto& child : children_) {
      if (child->Valid()) {
        min_heap_.push(child);
      }
    }
    current_ = min_heap_.empty() ? nullptr : min_heap_.top();
  }

  ~MergingIterator() {
    for (int i = 0; i < children_.size(); i++) {
      auto iter = children_[i];
      delete iter;
    }
    children_.clear();
  }

  bool Valid() const { return current_ != nullptr; }

  Status status() const {
    Status status;
    for (const auto& child : children_) {
      status = child->status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

  bool IsFinished(const std::string& prefix) {
    if (Valid() && (Key().compare(prefix) <= 0 || Key().substr(0, prefix.size()) == prefix)) {
      return false;
    }
    return true;
  }

  void SeekToFirst() {
    min_heap_.clear();
    max_heap_.clear();
    for (auto& child : children_) {
      child->SeekToFirst();
      if (child->Valid()) {
        min_heap_.push(child);
      }
    }
    direction_ = kForward;
    current_ = min_heap_.empty() ? nullptr : min_heap_.top();
  }

  void SeekToLast() {
    min_heap_.clear();
    max_heap_.clear();
    for (auto& child : children_) {
      child->SeekToLast();
      if (child->Valid()) {
        max_heap_.push(child);
      }
    }
    direction_ = kReverse;
    current_ = max_heap_.empty() ? nullptr : max_heap_.top();
  }

  void Seek(const std::string& target) {
    min_heap_.clear();
    max_heap_.clear();
    for (auto& child : children_) {
      child->Seek(target);
      if (child->Valid()) {
        min_heap_.push(child);
      }
    }
    direction_ = kForward;
    current_ = min_heap_.empty() ? nullptr : min_heap_.top();
  }

  void SeekForPrev(const std::string& start_key) {
    min_heap_.clear();
    max_heap_.clear();
    for (auto& child : children_) {
      child->SeekForPrev(start_key);
      if (child->Valid()) {
        max_heap_.push(child);
      }
    }
    direction_ = kReverse;
    current_ = max_heap_.empty() ? nullptr : max_heap_.top();
  }

  void Next() {
    assert(direction_ == kForward);
    current_->Next();
    if (current_->Valid()) {
      min_heap_.replace_top(current_);
    } else {
      min_heap_.pop();
    }
    current_ = min_heap_.empty() ? nullptr : min_heap_.top();
  }

  void Prev() {
    assert(direction_ == kReverse);
    current_->Prev();
    if (current_->Valid()) {
      max_heap_.replace_top(current_);
    } else {
      max_heap_.pop();
    }
    current_ = max_heap_.empty() ? nullptr : max_heap_.top();
  }

  std::string Key() { return current_->Key(); }

  std::string Value() { return current_->Value(); }

  Status status() {
    Status s;
    for (const auto& child : children_) {
      s = child->status();
      if (!s.ok()) {
        break;
      }
    }
    return s;
  }

  bool Valid() { return current_ != nullptr; }

private:
  enum Direction { kForward, kReverse };

  MergerMinIterHeap min_heap_;
  MergerMaxIterHeap max_heap_;
  std::vector<TypeIterator*> children_;
  TypeIterator* current_;
  Direction direction_;
};

} // end namespace storage

# endif
