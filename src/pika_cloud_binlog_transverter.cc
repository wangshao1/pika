// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_cloud_binlog_transverter.h"

#include <glog/logging.h>

#include <sstream>

#include "include/pika_command.h"
#include "pstd/include/pstd_coding.h"

const int PADDING_BINLOG_PROTOCOL_SIZE = 22;
const int SPACE_STROE_PARAMETER_LENGTH = 5;

std::string PikaCloudBinlogTransverter::BinlogEncode(uint32_t db_id, uint32_t rocksdb_id, uint32_t exec_time,
                                                     uint32_t term_id, uint32_t filenum, uint64_t offset,
                                                     const std::string& content) {
  std::string serialize_binlog;
  cloud::BinlogCloudItem binlog_item;
  binlog_item.set_db_id(db_id);
  binlog_item.set_rocksdb_id(rocksdb_id);
  binlog_item.set_exec_time(exec_time);
  binlog_item.set_term_id(term_id);
  binlog_item.set_file_num(filenum);
  binlog_item.set_offset(offset);
  binlog_item.set_content(content);
  binlog_item.SerializeToString(&serialize_binlog);
  return serialize_binlog;
}

bool PikaCloudBinlogTransverter::BinlogDecode(const std::string& binlog, cloud::BinlogCloudItem* binlog_item) {
  auto res = binlog_item->ParseFromString(binlog);
  if (!res) {
    LOG(ERROR) << "Failed to deserialize cloud binlog item";
    return false;
  }
  return true;
}

std::string PikaCloudBinlogTransverter::ConstructPaddingBinlog(uint32_t parameter_len) {
  std::string binlog;
  cloud::BinlogCloudItem binlog_item;
  if (parameter_len < 0) {
    return {};
  }

  std::string content;
  RedisAppendLen(content, 2, "*");
  RedisAppendLen(content, 7, "$");
  RedisAppendContent(content, "padding");

  std::string parameter_len_str;
  std::ostringstream os;
  os << parameter_len;
  std::istringstream is(os.str());
  is >> parameter_len_str;
  if (parameter_len_str.size() > SPACE_STROE_PARAMETER_LENGTH) {
    return {};
  }

  content.append("$");
  content.append(SPACE_STROE_PARAMETER_LENGTH - parameter_len_str.size(), '0');
  content.append(parameter_len_str);
  content.append(kNewLine);
  RedisAppendContent(content, std::string(parameter_len, '*'));

  BinlogEncode(0, 0, 0, 0, 0, 0, content);
  return binlog;
}

bool PikaCloudBinlogTransverter::BinlogItemWithoutContentDecode(const std::string& binlog,
                                                                cloud::BinlogCloudItem* binlog_item) {
  auto res = binlog_item->ParseFromString(binlog);
  if (!res) {
    LOG(ERROR) << "Failed to deserialize cloud binlog item";
    return false;
  }
  binlog_item->set_content("");
  return true;
}
