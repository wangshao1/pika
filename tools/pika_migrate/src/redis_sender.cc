// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include "include/redis_sender.h"

#include <time.h>
#include <unistd.h>

#include <unordered_set>

#include <glog/logging.h>

#include "include/pika_conf.h"
#include "pstd/include/env.h"

extern PikaConf* g_pika_conf;

static time_t kCheckDiff = 1;
static const int64_t kSlowLogThresholdUs = 3000;

RedisSender::RedisSender(int id, std::string ip, int64_t port, std::string user, std::string password):
  id_(id),
  cli_(NULL),
  ip_(ip),
  port_(port),
  user_(user),
  password_(password),
  should_exit_(false),
  elements_(0) {
  last_write_time_ = ::time(NULL);
}

RedisSender::~RedisSender() {
  LOG(INFO) << "RedisSender thread " << id_ << " exit!!!";
}

void RedisSender::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = std::shared_ptr<net::NetCli>(net::NewRedisCli());
    cli_->set_connect_timeout(1000);
    cli_->set_recv_timeout(10000);
    cli_->set_send_timeout(10000);
    pstd::Status s = cli_->Connect(ip_, port_);
    if (!s.ok()) {
      LOG(WARNING) << "Can not connect to " << ip_ << ":" << port_ << ", status: " << s.ToString();
      cli_ = NULL;
      sleep(3);
      continue;
    } else {
      // Connect success
      // LOG(INFO) << "RedisSender thread " << id_ << "Connect to redis(" << ip_ << ":" << port_ << ") success";
      // Authentication
      if (!password_.empty()) {
        net::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("AUTH");
        argv.push_back(password_);
        net::SerializeRedisCommand(argv, &cmd);
        pstd::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (resp[0] == "OK") {
          } else {
            LOG(FATAL) << "Connect to redis(" << ip_ << ":" << port_ << ") Invalid password";
            cli_->Close();
            cli_ = NULL;
            should_exit_ = true;
            return;
          }
        } else {
          LOG(WARNING) << "send auth failed: " << s.ToString();
          cli_->Close();
          cli_ = NULL;
          continue;
        }
      } else {
        // If forget to input password
        net::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        net::SerializeRedisCommand(argv, &cmd);
        pstd::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              LOG(FATAL) << "Ping redis(" << ip_ << ":" << port_ << ") NOAUTH Authentication required";
              cli_->Close();
              cli_ = NULL;
              should_exit_ = true;
              return;
            }
          } else {
            LOG(WARNING) << s.ToString();
            cli_->Close();
            cli_ = NULL;
          }
        }
      }
    }
  }
}

void RedisSender::Stop() {
  set_should_stop();
  should_exit_ = true;
  rsignal_.notify_all();
  wsignal_.notify_all();
}

void RedisSender::SendRedisCommand(const std::string &key, const std::string &command) {
  const auto start_us = pstd::NowMicros();
  std::unique_lock lock(signal_mutex_);
  const auto signal_mutex_lock_us = pstd::NowMicros();
  wsignal_.wait(lock, [this]() { return commandQueueSize() < 100000 || should_exit_; });
  const auto signal_wait_us = pstd::NowMicros();
  lock.unlock();

  if (!should_exit_) {
    const auto queue_mutex_start_us = pstd::NowMicros();
    {
      std::lock_guard l(command_queue_mutex_);
      commands_queue_.push(std::make_pair(key, command));
    }
    const auto queue_push_us = pstd::NowMicros();
    rsignal_.notify_one();
    const auto end_us = pstd::NowMicros();

    if (end_us - start_us >= kSlowLogThresholdUs) {
      LOG(INFO) << "RedisSender enqueue slow, id: " << id_
                << ", total cost: " << (end_us - start_us) / 1000 << " ms"
                << ", signal_mutex lock: " << (signal_mutex_lock_us - start_us) / 1000 << " ms"
                << ", signal wait: " << (signal_wait_us - signal_mutex_lock_us) / 1000 << " ms"
                << ", queue push: " << (queue_push_us - queue_mutex_start_us) / 1000 << " ms"
                << ", notify: " << (end_us - queue_push_us) / 1000 << " ms"
                << ", command size: " << command.size();
    }
  }
}

int RedisSender::SendCommand(std::string &command) {
  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
    int ret = cli_->CheckAliveness();
    if (ret < 0) {
      cli_ = nullptr;
      ConnectRedis();
    }
    last_write_time_ = now;
  }

  // Send command
  int idx = 0;
  do {
    pstd::Status s = cli_->Send(&command);

    if (s.ok()) {
      cli_->Recv(nullptr);
      return 0;
    }

    cli_->Close();
    cli_ = NULL;
    ConnectRedis();
  } while(++idx < 3);
  LOG(FATAL) << "RedisSender " << id_ << " fails to send redis command " << command << ", times: " << idx << ", error: " << "send command failed";
  return -1;
}

// Pipeline send: write the whole batch first, then read one reply per command.
// Redis guarantees replies come back in request order, so batching over a
// single connection does not disturb the per-key ordering guarantee.
// If any send/recv in the fast path fails, fall back to per-command sending
// (with its own reconnect+retry) so no command is lost and order is kept.
int RedisSender::SendCommands(std::vector<std::string> &commands) {
  if (commands.empty()) {
    return 0;
  }

  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
    int ret = cli_->CheckAliveness();
    if (ret < 0) {
      cli_ = nullptr;
      ConnectRedis();
    }
    last_write_time_ = now;
  }

  const auto start_us = pstd::NowMicros();
  // Fast path: send the whole batch, then receive one reply per command.
  bool ok = true;
  for (auto &command : commands) {
    pstd::Status s = cli_->Send(&command);
    if (!s.ok()) {
      ok = false;
      break;
    }
  }
  if (ok) {
    for (size_t i = 0; i < commands.size(); ++i) {
      pstd::Status s = cli_->Recv(nullptr);
      if (!s.ok()) {
        ok = false;
        break;
      }
    }
  }
  const auto end_us = pstd::NowMicros();
  if (end_us - start_us >= kSlowLogThresholdUs) {
    LOG(INFO) << "RedisSender send slow, id: " << id_
              << ", commands: " << commands.size()
              << ", cost: " << (end_us - start_us) / 1000 << " ms"
              << ", status: " << ok;
  }
  if (ok) {
    return 0;
  }

  // Slow path: connection got into a bad/undefined state (a partial batch may
  // have been sent). Drop it and resend every command in order one-by-one.
  cli_->Close();
  cli_ = NULL;
  ConnectRedis();
  for (auto &command : commands) {
    if (SendCommand(command) != 0) {
      return -1;
    }
  }
  return 0;
}

void *RedisSender::ThreadMain() {
  LOG(INFO) << "Start redis sender " << id_ << " thread...";
  // sleep(15);
  int ret = 0;

  ConnectRedis();

  const size_t pipeline_size = static_cast<size_t>(g_pika_conf->redis_pipeline_size());
  const int64_t pipeline_wait_us = g_pika_conf->redis_pipeline_wait_us();

  while (!should_exit_) {
    {
      std::unique_lock lock(signal_mutex_);
      while (commandQueueSize() == 0 && !should_exit_) {
        rsignal_.wait_for(lock, std::chrono::milliseconds(100));
      }
    }

    if (should_exit_) {
      break;
    }

    if (commandQueueSize() == 0) {
      continue;
    }

    // Micro-batching window: the producer side (binlog replay / migrator) often
    // enqueues one command at a time, so without waiting the queue usually holds
    // a single command when we wake up and every network round-trip carries just
    // one command. Wait up to pipeline_wait_us for the queue to fill toward
    // pipeline_size, so one round-trip amortizes many commands. Flush early the
    // moment we have a full batch; exit the wait promptly on shutdown.
    if (pipeline_wait_us > 0 && commandQueueSize() < pipeline_size) {
      const int64_t deadline_us = pstd::NowMicros() + pipeline_wait_us;
      while (!should_exit_
             && commandQueueSize() < pipeline_size
             && pstd::NowMicros() < deadline_us) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
    }

    // Build one pipeline batch. Rule: the same key must not appear twice in a
    // batch. When a duplicate key is seen we stop the batch there and leave
    // that command for the next batch, so commands for the same key are always
    // sent in separate, serialized batches. This preserves per-key write order
    // even when the target proxy fans out to backends over multiple
    // connections (codis-like), where a single connection no longer implies
    // ordering. An empty key is treated as a standalone single-command batch.
    std::vector<std::string> batch;
    std::unordered_set<std::string> seen_keys;
    {
      std::lock_guard l(command_queue_mutex_);
      while (!commands_queue_.empty() && batch.size() < pipeline_size) {
        const std::string& next_key = commands_queue_.front().first;

        if (next_key.empty()) {
          // Non key-scoped command: only allow it as the first (and only) entry
          // of a batch, then stop so it is flushed on its own.
          if (!batch.empty()) {
            break;
          }
          batch.push_back(commands_queue_.front().second);
          elements_++;
          commands_queue_.pop();
          break;
        }

        // Key already in this batch -> stop, defer it to the next batch.
        if (seen_keys.find(next_key) != seen_keys.end()) {
          break;
        }

        seen_keys.insert(next_key);
        batch.push_back(commands_queue_.front().second);
        elements_++;
        commands_queue_.pop();
      }
    }

    wsignal_.notify_all();
    ret = SendCommands(batch);
  }

  LOG(INFO) << "RedisSender thread " << id_ << " complete";
  cli_ = NULL;
  return NULL;
}