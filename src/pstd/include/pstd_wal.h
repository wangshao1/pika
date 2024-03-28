// Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef __PSTD_WAL_H__
#define __PSTD_WAL_H__

#include "pstd/include/pstd_status.h"
#include "pstd/include/noncopyable.h"

namespace pstd {

// virutal base class for wal writer
class WalWriter : public noncopyable {
public:
  virtual ~WalWriter() {}
  virtual Status Put(const std::string& item, uint32_t db_id, uint32_t rocksdb_id， uint32_t type) = 0;
};
} // namespace pstd

#endif  // __PSTD_WAL_H__
