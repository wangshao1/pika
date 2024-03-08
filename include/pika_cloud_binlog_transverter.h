// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLOUD_BINLOG_TRANSVERTER_H_
#define PIKA_CLOUD_BINLOG_TRANSVERTER_H_

#include <glog/logging.h>
#include <cstdint>
#include <iostream>
#include <vector>
#include "pika_cloud_binlog.pb.h"

const int PADDING_BINLOG_PROTOCOL_SIZE = 22;
const int SPACE_STROE_PARAMETER_LENGTH = 5;

class PikaCloudBinlogTransverter {
 public:
  PikaCloudBinlogTransverter() = default;
  static std::string BinlogEncode(uint32_t db_id, uint32_t rocksdb_id, uint32_t exec_time, uint32_t term_id,
                                  uint32_t filenum, uint64_t offset, const std::string& content);

  static bool BinlogDecode(const std::string& binlog, cloud::BinlogCloudItem* binlog_item);

  static std::string ConstructPaddingBinlog(uint32_t size);

  static bool BinlogItemWithoutContentDecode(const std::string& binlog,
                                             cloud::BinlogCloudItem* binlog_item);
};

#endif
