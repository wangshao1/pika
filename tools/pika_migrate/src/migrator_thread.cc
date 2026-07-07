// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/migrator_thread.h"

#include <unistd.h>

#include <vector>
#include <functional>
#define GLOG_USE_GLOG_EXPORT
#include <glog/logging.h>

#include "storage/storage.h"
#include "src/redis_strings.h"
#include "src/redis_lists.h"
#include "src/redis_hashes.h"
#include "src/redis_sets.h"
#include "src/redis_zsets.h"
#include "src/scope_snapshot.h"
#include "src/strings_value_format.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/env.h"

#include "include/pika_conf.h"

const int64_t MAX_BATCH_NUM = 30000;
static const int64_t kSlowLogThresholdUs = 3000;

extern PikaConf* g_pika_conf;

MigratorThread::~MigratorThread() {
}

void MigratorThread::MigrateStringsDB() {
  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t cursor = 0;
  std::vector<storage::KeyValueTTL> kvs;
  while (true) {
    auto start_us = pstd::NowMicros();
    // Read key + value + ttl in a single scan pass, avoiding the extra
    // Get()/TTL() point lookups that used to run once per key.
    cursor = storage_->ScanStringsWithValue(cursor, "*", scan_batch_num, &kvs);
    auto scan_end_us = pstd::NowMicros();
    if (scan_end_us - start_us >= kSlowLogThresholdUs) {
      LOG(INFO) << "String scan slow, commands: " << kvs.size()
                << ", cost: " << (scan_end_us - start_us) / 1000 << " ms";
    }

    for (const auto& kv : kvs) {
      net::RedisCmdArgsType argv;
      std::string cmd;

      argv.push_back("SET");
      argv.push_back(kv.key);
      argv.push_back(kv.value);

      // kv.ttl: >0 remaining seconds, -1 no expiration (0/negative are skipped).
      if (kv.ttl > 0) {
        argv.push_back("EX");
        argv.push_back(std::to_string(kv.ttl));
      }

      net::SerializeRedisCommand(argv, &cmd);
      PlusNum();
      auto dispatch_us = pstd::NowMicros();
      DispatchKey(cmd, kv.key);
      auto end_us = pstd::NowMicros();
      if (end_us - dispatch_us >= kSlowLogThresholdUs) {
        LOG(INFO) << "DispatchKey slow, key: " << kv.key
                  << ", cost: " << (end_us - dispatch_us) / 1000 << " ms"
                  << ", command size: " << cmd.size();
      }
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateListsDB() {
  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  std::vector<std::string> keys;

  while (true) {
    cursor = storage_->Scan(storage::DataType::kLists, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      int64_t pos = 0;
      std::vector<std::string> nodes;
      // Read the first element batch together with the list's ttl in a single
      // meta lookup, avoiding a separate TTL() point lookup per key.
      // LRangeWithTTL returns ttl: >0 remaining seconds, -1 permanent, -2 expired.
      ttl = -1;
      storage::Status s = storage_->LRangeWithTTL(
          key, pos, pos + g_pika_conf->sync_batch_num() - 1, &nodes, &ttl);
      if (!s.ok()) {
        LOG(WARNING) << "db->LRangeWithTTL(key:" << key << ", pos:" << pos
          << ", batch size: " << g_pika_conf->sync_batch_num() << ") = " << s.ToString();
        continue;
      }

      while (s.ok() && !should_exit_ && !nodes.empty()) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("RPUSH");
        argv.push_back(key);
        for (const auto& node : nodes) {
          argv.push_back(node);
        }

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);

        pos += g_pika_conf->sync_batch_num();
        nodes.clear();
        s = storage_->LRange(key, pos, pos + g_pika_conf->sync_batch_num() - 1, &nodes);
        if (!s.ok()) {
          LOG(WARNING) << "db->LRange(key:" << key << ", pos:" << pos
            << ", batch size:" << g_pika_conf->sync_batch_num() << ") = " << s.ToString();
        }
      }

      if (s.ok() && ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }
    }


    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateHashesDB() {
  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  std::vector<std::string> keys;
  std::map<storage::DataType, int64_t> type_timestamp;
  std::map<storage::DataType, rocksdb::Status> type_status;

  while (true) {
    cursor = storage_->Scan(storage::DataType::kHashes, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      // Scan the hash's fields in cursor-paged batches so peak memory stays at
      // one batch instead of the whole hash (large keys would otherwise be read
      // in full). ttl is fetched once per key outside the paging loop.
      int64_t field_cursor = 0;
      bool read_ok = true;
      do {
        std::vector<storage::FieldValue> fvs;
        storage::Status s = storage_->HScan(key, field_cursor, "*", g_pika_conf->sync_batch_num(),
                                             &fvs, &field_cursor);
        if (s.IsNotFound()) {
          // Key absent or already expired: nothing to migrate, skip quietly.
          break;
        }
        if (!s.ok()) {
          LOG(WARNING) << "db->HScan(key:" << key << ", cursor:" << field_cursor << ") = " << s.ToString();
          read_ok = false;
          break;
        }
        if (fvs.empty()) {
          continue;
        }

        net::RedisCmdArgsType argv;
        std::string cmd;
        argv.reserve(2 + fvs.size() * 2);
        argv.push_back("HMSET");
        argv.push_back(key);
        for (auto& fv : fvs) {
          if (should_exit_) {
            break;
          }
          argv.push_back(std::move(fv.field));
          argv.push_back(std::move(fv.value));
        }

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      } while (!should_exit_ && field_cursor != 0);

      if (!read_ok) {
        continue;
      }

      ttl = -1;
      type_status.clear();
      type_timestamp = storage_->TTL(key, &type_status);
      if (type_timestamp[storage::kHashes] != -2) {
        ttl = type_timestamp[storage::kHashes];
      }

      if (ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateSetsDB() {
  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  std::vector<std::string> keys;
  std::map<storage::DataType, int64_t> type_timestamp;
  std::map<storage::DataType, rocksdb::Status> type_status;

  while (true) {
    cursor = storage_->Scan(storage::DataType::kSets, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      // Scan the set's members in cursor-paged batches so peak memory stays at
      // one batch instead of the whole set. ttl is fetched once per key outside
      // the paging loop.
      int64_t member_cursor = 0;
      bool read_ok = true;
      do {
        std::vector<std::string> members;
        storage::Status s = storage_->SScan(key, member_cursor, "*", g_pika_conf->sync_batch_num(),
                                             &members, &member_cursor);
        if (s.IsNotFound()) {
          // Key absent or already expired: nothing to migrate, skip quietly.
          break;
        }
        if (!s.ok()) {
          LOG(WARNING) << "db->SScan(key:" << key << ", cursor:" << member_cursor << ") = " << s.ToString();
          read_ok = false;
          break;
        }
        if (members.empty()) {
          continue;
        }

        std::string cmd;
        net::RedisCmdArgsType argv;
        argv.reserve(2 + members.size());
        argv.push_back("SADD");
        argv.push_back(key);
        for (auto& member : members) {
          if (should_exit_) {
            break;
          }
          argv.push_back(std::move(member));
        }

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      } while (!should_exit_ && member_cursor != 0);

      if (!read_ok) {
        continue;
      }

      ttl = -1;
      type_status.clear();
      type_timestamp = storage_->TTL(key, &type_status);
      if (type_timestamp[storage::kSets] != -2) {
        ttl = type_timestamp[storage::kSets];
      }

      if (ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);

      }
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateZsetsDB() {
  int64_t scan_batch_num = g_pika_conf->sync_batch_num() * 10;
  if (MAX_BATCH_NUM < scan_batch_num) {
    if (g_pika_conf->sync_batch_num() < MAX_BATCH_NUM) {
      scan_batch_num = MAX_BATCH_NUM;
    } else {
      scan_batch_num = g_pika_conf->sync_batch_num() * 2;
    }
  }

  int64_t ttl = -1;
  int64_t cursor = 0;
  std::vector<std::string> keys;
  std::map<storage::DataType, int64_t> type_timestamp;
  std::map<storage::DataType, rocksdb::Status> type_status;

  while (true) {
    cursor = storage_->Scan(storage::DataType::kZSets, cursor, "*", scan_batch_num, &keys);

    for (const auto& key : keys) {
      // Scan the zset's members in cursor-paged batches so peak memory stays at
      // one batch instead of the whole zset. ttl is fetched once per key outside
      // the paging loop.
      int64_t member_cursor = 0;
      bool read_ok = true;
      do {
        std::vector<storage::ScoreMember> score_members;
        storage::Status s = storage_->ZScan(key, member_cursor, "*", g_pika_conf->sync_batch_num(),
                                             &score_members, &member_cursor);
        if (s.IsNotFound()) {
          // Key absent or already expired: nothing to migrate, skip quietly.
          break;
        }
        if (!s.ok()) {
          LOG(WARNING) << "db->ZScan(key:" << key << ", cursor:" << member_cursor << ") = " << s.ToString();
          read_ok = false;
          break;
        }
        if (score_members.empty()) {
          continue;
        }

        net::RedisCmdArgsType argv;
        std::string cmd;
        argv.reserve(2 + score_members.size() * 2);
        argv.push_back("ZADD");
        argv.push_back(key);
        for (auto& sm : score_members) {
          if (should_exit_) {
            break;
          }

          // Format score with d2string (%.17g + integer/nan/inf/-0 handling),
          // matching the source node's ZADD/ZRANGE output. std::to_string uses
          // %f (6 decimals) and would lose precision.
          char score_buf[32];
          int64_t score_len = pstd::d2string(score_buf, sizeof(score_buf), sm.score);
          argv.push_back(std::string(score_buf, score_len));
          argv.push_back(std::move(sm.member));
        }

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      } while (!should_exit_ && member_cursor != 0);

      if (!read_ok) {
        continue;
      }

      ttl = -1;
      type_status.clear();
      type_timestamp = storage_->TTL(key, &type_status);
      if (type_timestamp[storage::kZSets] != -2) {
        ttl = type_timestamp[storage::kZSets];
      }

      if (ttl > 0) {
        net::RedisCmdArgsType argv;
        std::string cmd;

        argv.push_back("EXPIRE");
        argv.push_back(key);
        argv.push_back(std::to_string(ttl));

        net::SerializeRedisCommand(argv, &cmd);
        PlusNum();
        DispatchKey(cmd, key);
      }
    }

    if (!cursor) {
      break;
    }
  }
}

void MigratorThread::MigrateDB() {
  switch (int(type_)) {
    case int(storage::kStrings) : {
      MigrateStringsDB();
      break;
    }

    case int(storage::kLists) : {
      MigrateListsDB();
      break;
    }

    case int(storage::kHashes) : {
      MigrateHashesDB();
      break;
    }

    case int(storage::kSets) : {
      MigrateSetsDB();
      break;
    }

    case int(storage::kZSets) : {
      MigrateZsetsDB();
      break;
    }

    default: {
      LOG(WARNING) << "illegal db type " << type_;
      break;
    }
  }
}

void MigratorThread::DispatchKey(const std::string &command, const std::string& key) {
  thread_index_ = (thread_index_ + 1) % thread_num_;
  size_t idx = thread_index_;
  if (key.size()) { // no empty
    idx = std::hash<std::string>()(key) % thread_num_;
  }
  (*senders_)[idx]->SendRedisCommand(key, command);
}

const char* GetDBTypeString(int type) {
  switch (type) {
    case int(storage::kStrings) : {
	  return "storage::kStrings";
    }

    case int(storage::kLists) : {
	  return "storage::kLists";
    }

    case int(storage::kHashes) : {
	  return "storage::kHashes";
    }

    case int(storage::kSets) : {
	  return "storage::kSets";
    }

    case int(storage::kZSets) : {
	  return "storage::kZSets";
    }

    default: {
	  return "storage::Unknown";
    }
  }
}

void *MigratorThread::ThreadMain() {
  MigrateDB();
  should_exit_ = true;
  LOG(INFO) << GetDBTypeString(type_) << " keys have been dispatched completly";
  return NULL;
}