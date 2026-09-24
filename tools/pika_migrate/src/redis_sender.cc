// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include "include/redis_sender.h"

#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <unordered_set>

#include <glog/logging.h>

#include "include/pika_conf.h"
#include "pstd/include/env.h"

extern PikaConf* g_pika_conf;

static time_t kCheckDiff = 1;
static const int64_t kSlowLogThresholdUs = 3000;
// Emit one throughput summary line per sender thread every 10 seconds, so the
// send-side bottleneck (batch fill vs drain speed) is visible without waiting
// for the whole migration to finish.
static const int64_t kThroughputLogPeriodUs = 10 * 1000 * 1000;
// Caps for turning a serialized command into a readable, single-line log
// string, so a large value (or a huge multi-field command) can never flood the
// log. Only used on failure paths.
static const size_t kLogMaxArgs = 8;
static const size_t kLogMaxArgLen = 64;

// Parse a serialized RESP command back into a readable "CMD arg arg ..." form
// for logging. Truncates the number of args and the length of each arg. This is
// best-effort: if the buffer does not look like RESP we fall back to a short
// hex-free preview so the log line is still bounded.
static std::string CommandToReadable(const std::string& command) {
  std::string out;
  size_t i = 0;
  // RESP array header: *<count>\r\n
  if (command.empty() || command[0] != '*') {
    out.assign(command, 0, kLogMaxArgLen);
    if (command.size() > kLogMaxArgLen) {
      out += "...";
    }
    return out;
  }

  size_t nl = command.find('\n', i);
  if (nl == std::string::npos) {
    return "<unparsable>";
  }
  long argc = strtol(command.c_str() + 1, nullptr, 10);
  i = nl + 1;

  size_t printed = 0;
  for (long a = 0; a < argc; ++a) {
    if (i >= command.size() || command[i] != '$') {
      break;
    }
    long len = strtol(command.c_str() + i + 1, nullptr, 10);
    nl = command.find('\n', i);
    if (nl == std::string::npos || len < 0) {
      break;
    }
    size_t arg_start = nl + 1;
    if (arg_start + static_cast<size_t>(len) > command.size()) {
      break;
    }

    if (printed >= kLogMaxArgs) {
      out += " ...(" + std::to_string(argc) + " args total)";
      break;
    }
    if (!out.empty()) {
      out += ' ';
    }
    size_t show = std::min(static_cast<size_t>(len), kLogMaxArgLen);
    out.append(command, arg_start, show);
    if (static_cast<size_t>(len) > kLogMaxArgLen) {
      out += "...";
    }
    ++printed;
    i = arg_start + static_cast<size_t>(len) + 2;  // skip arg + trailing \r\n
  }
  return out;
}

RedisSender::RedisSender(int id, std::string ip, int64_t port, std::string user, std::string password):
  id_(id),
  cli_(NULL),
  ip_(ip),
  port_(port),
  user_(user),
  password_(password),
  should_exit_(false),
  elements_(0),
  replies_received_(0) {
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
      LOG(WARNING) << "RedisSender " << id_ << " can not connect to " << ip_ << ":" << port_
                   << ", status: " << s.ToString();
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
            LOG(WARNING) << "RedisSender " << id_ << " ping recv failed, target: " << ip_ << ":" << port_
                         << ", status: " << s.ToString();
            cli_->Close();
            cli_ = NULL;
          }
        } else {
          LOG(WARNING) << "send ping failed to " << ip_ << ":" << port_ << ", status: " << s.ToString();
          cli_->Close();
          cli_ = NULL;
        }
      }
    }
  }
}

void RedisSender::Stop() {
  set_should_stop();
  should_exit_ = true;
  graceful_exit_ = false;
  rsignal_.notify_all();
  wsignal_.notify_all();
}

void RedisSender::GracefulStop() {
  // Ask the thread to stop, but only after the queue is fully drained. Order
  // matters: set graceful_exit_ before should_exit_ so ThreadMain never observes
  // should_exit_ == true without graceful_exit_ == true and skips the drain.
  graceful_exit_ = true;
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
  } else {
    // Dropped on the floor because the sender is shutting down. The caller
    // (DispatchKey) has no return channel, so this is otherwise invisible.
    LOG(WARNING) << "RedisSender " << id_ << " dropping command because sender is exiting, target: " << ip_ << ":"
                 << port_ << ", key: " << key << ", command size: " << command.size();
  }
}

int RedisSender::SendCommand(const std::string &key, std::string &command) {
  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
    int ret = cli_->CheckAliveness();
    if (ret < 0) {
      LOG(WARNING) << "RedisSender " << id_ << " connection to " << ip_ << ":" << port_
                   << " not alive before send, reconnecting, key: " << key;
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
      // Read and inspect the target's reply. A transport-level error here means
      // the write may not have landed; an application-level error reply (e.g.
      // -ERR / -WRONGTYPE / -OOM / -MOVED) means the target REJECTED the write
      // even though the connection is healthy. Both are causes of src/dst
      // divergence, so log them at WARNING. Behavior is unchanged (still returns
      // success) — logging only, so the diagnosis does not alter the data flow.
      net::RedisCmdArgsType reply;
      pstd::Status rs = cli_->Recv(&reply);
      if (rs.ok()) {
        replies_received_++;
      }
      if (!rs.ok()) {
        LOG(WARNING) << "RedisSender " << id_ << " recv reply failed after send, target: " << ip_ << ":" << port_
                     << ", status: " << rs.ToString() << ", key: " << key
                     << ", command: " << CommandToReadable(command);
      } else if (!reply.empty() && !reply[0].empty() && reply[0][0] == '-') {
        LOG(WARNING) << "RedisSender " << id_ << " target returned error reply, target: " << ip_ << ":" << port_
                     << ", reply: " << reply[0] << ", key: " << key
                     << ", command: " << CommandToReadable(command);
      }
      return 0;
    }

    LOG(WARNING) << "RedisSender " << id_ << " send command failed, target: " << ip_ << ":" << port_
                 << ", status: " << s.ToString() << ", retry: " << idx << ", key: " << key
                 << ", command: " << CommandToReadable(command);
    cli_->Close();
    cli_ = NULL;
    ConnectRedis();
  } while(++idx < 3);
  LOG(FATAL) << "RedisSender " << id_ << " fails to send redis command, key: " << key
             << ", command: " << CommandToReadable(command) << ", times: " << idx << ", error: send command failed";
  return -1;
}

// Pipeline send: write the whole batch first, then read one reply per command.
// Redis guarantees replies come back in request order, so batching over a
// single connection does not disturb the per-key ordering guarantee.
// If any send/recv in the fast path fails, fall back to per-command sending
// (with its own reconnect+retry) so no command is lost and order is kept.
int RedisSender::SendCommands(std::vector<std::pair<std::string, std::string>> &commands) {
  if (commands.empty()) {
    return 0;
  }

  time_t now = ::time(NULL);
  if (kCheckDiff < now - last_write_time_) {
    int ret = cli_->CheckAliveness();
    if (ret < 0) {
      LOG(WARNING) << "RedisSender " << id_ << " connection to " << ip_ << ":" << port_
                   << " not alive before batch send, reconnecting";
      cli_ = nullptr;
      ConnectRedis();
    }
    last_write_time_ = now;
  }

  const auto start_us = pstd::NowMicros();
  // Fast path: send the whole batch, then receive one reply per command.
  bool ok = true;
  for (auto &kv : commands) {
    pstd::Status s = cli_->Send(&kv.second);
    if (!s.ok()) {
      LOG(WARNING) << "RedisSender " << id_ << " batch send failed (fast path), target: " << ip_ << ":" << port_
                   << ", status: " << s.ToString() << ", batch size: " << commands.size()
                   << ", key: " << kv.first << ", command: " << CommandToReadable(kv.second)
                   << ", will fall back to per-command resend";
      ok = false;
      break;
    }
  }
  const auto send_done_us = pstd::NowMicros();
  if (ok) {
    for (size_t i = 0; i < commands.size(); ++i) {
      net::RedisCmdArgsType reply;
      pstd::Status s = cli_->Recv(&reply);
      if (!s.ok()) {
        LOG(WARNING) << "RedisSender " << id_ << " batch recv failed (fast path), target: " << ip_ << ":" << port_
                     << ", status: " << s.ToString() << ", reply idx: " << i << "/" << commands.size()
                     << ", key: " << commands[i].first << ", command: " << CommandToReadable(commands[i].second)
                     << ", will fall back to per-command resend";
        ok = false;
        break;
      }
      replies_received_++;
      // Connection is healthy but the target rejected this write at the
      // application layer. This does NOT trigger the fallback resend, so log it
      // so silently-dropped writes are visible when src/dst diverge.
      if (!reply.empty() && !reply[0].empty() && reply[0][0] == '-') {
        LOG(WARNING) << "RedisSender " << id_ << " target returned error reply (fast path), target: " << ip_ << ":"
                     << port_ << ", reply: " << reply[0] << ", reply idx: " << i << "/" << commands.size()
                     << ", key: " << commands[i].first << ", command: " << CommandToReadable(commands[i].second);
      }
    }
  }
  const auto end_us = pstd::NowMicros();
  if (end_us - start_us >= kSlowLogThresholdUs) {
    LOG(INFO) << "RedisSender batch slow, id: " << id_
              << ", commands: " << commands.size()
              << ", cost: " << (end_us - start_us) / 1000 << " ms"
              << ", write phase: " << (send_done_us - start_us) / 1000 << " ms"
              << ", recv phase: " << (end_us - send_done_us) / 1000 << " ms"
              << ", avg per cmd: " << (end_us - start_us) / commands.size() << " us"
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
  for (auto &kv : commands) {
    if (SendCommand(kv.first, kv.second) != 0) {
      LOG(WARNING) << "RedisSender " << id_ << " per-command resend failed (slow path), target: " << ip_ << ":"
                   << port_ << ", batch size: " << commands.size() << ", key: " << kv.first
                   << ", command: " << CommandToReadable(kv.second);
      return -1;
    }
  }
  return 0;
}

void *RedisSender::ThreadMain() {
  LOG(INFO) << "Start redis sender " << id_ << " thread...";
  // sleep(15);
  int ret = 0;
  // Throughput summary counters (per sender thread, printed every 10s and at
  // exit): commands fully sent, SendCommands passes, and pass outcome split.
  int64_t replies_received_total = 0;
  int64_t window_passes_total = 0;
  int64_t failed_passes_total = 0;

  ConnectRedis();

  const auto thread_start_us = pstd::NowMicros();
  auto last_throughput_log_us = thread_start_us;
  int64_t last_logged_replies = 0;

  const size_t pipeline_size = static_cast<size_t>(g_pika_conf->redis_pipeline_size());
  const int64_t pipeline_wait_us = g_pika_conf->redis_pipeline_wait_us();
  // How many batches a single pass may stage (see the batch build below). 1
  // keeps the strict single-batch behaviour. Clamped again here so an
  // out-of-range value can never shrink the window to zero.
  int pipeline_window_conf = g_pika_conf->redis_pipeline_window();
  if (pipeline_window_conf < 1) {
    pipeline_window_conf = 1;
  }
  const size_t pipeline_window = static_cast<size_t>(pipeline_window_conf);
  // Commands one pass can stage across the whole window. The micro-batching
  // wait below targets this instead of a single batch, otherwise it would stop
  // waiting as soon as the first batch is full and the window would stay empty.
  const size_t window_capacity = pipeline_size * pipeline_window;
  // Effective batching config: needed to read the throughput summary below
  // (avg cmds per pass is only meaningful against these limits).
  LOG(INFO) << "RedisSender " << id_ << " pipeline config, batch size: " << pipeline_size
            << ", window batches: " << pipeline_window
            << ", window capacity: " << window_capacity
            << ", wait us: " << pipeline_wait_us;

  while (!should_exit_ || (graceful_exit_ && commandQueueSize() > 0)) {
    {
      std::unique_lock lock(signal_mutex_);
      while (commandQueueSize() == 0 && !should_exit_) {
        rsignal_.wait_for(lock, std::chrono::milliseconds(100));
      }
    }

    // Stop decision:
    //  - forced stop (should_exit_ && !graceful_exit_): leave now, dropping the
    //    queue (the caller has declared the queued data no longer matters).
    //  - graceful stop (should_exit_ && graceful_exit_): keep going until the
    //    queue is empty, then leave, so no enqueued command is dropped.
    if (should_exit_ && !graceful_exit_) {
      break;
    }
    if (should_exit_ && graceful_exit_ && commandQueueSize() == 0) {
      break;
    }

    if (commandQueueSize() == 0) {
      continue;
    }

    // Micro-batching window: the producer side (binlog replay / migrator) often
    // enqueues one command at a time, so without waiting the queue usually holds
    // a single command when we wake up and every network round-trip carries just
    // one command. Wait up to pipeline_wait_us for the queue to fill toward
    // the window capacity, so one round-trip amortizes many commands. Flush
    // early the moment the window can be fully staged; exit the wait promptly
    // on shutdown.
    if (pipeline_wait_us > 0 && commandQueueSize() < window_capacity) {
      const int64_t deadline_us = pstd::NowMicros() + pipeline_wait_us;
      while (!(should_exit_ && !graceful_exit_)
             && commandQueueSize() < window_capacity
             && pstd::NowMicros() < deadline_us) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
    }

    // Build the pipeline window: up to pipeline_window batches per pass.
    //
    // Rule (unchanged): a batch must not hold the same key twice, because the
    // target proxy (codis-like) can fan one connection's commands out to
    // different backends, where a single connection no longer implies ordering.
    // The old code truncated the batch at the first duplicate key, which for the
    // SET k v / EXPIREAT k t pattern emitted by the migrator collapsed almost
    // every batch to 1-2 commands. With a window a duplicate key is placed into
    // the next batch that does not hold it, so the batches stay full while the
    // per-key rule still holds.
    //
    // Order safety: the window is sent batch by batch in index order and each
    // SendCommands() waits for every reply before the next batch goes out, so at
    // most one batch is ever in flight. Inside a pass a key lands in strictly
    // increasing batch indices (every earlier batch either already holds that
    // key or is full, which is why it was not chosen for the previous command of
    // this key), and across passes the previous window is fully acknowledged
    // first. So commands for the same key always reach the target in enqueue
    // order, exactly as with pipeline_window = 1.
    //
    // A command with an empty key is not key-scoped: it travels alone in its own
    // batch and ends the pass, so it can never be reordered behind commands that
    // were enqueued after it.
    std::vector<std::vector<std::pair<std::string, std::string>>> batches;
    std::vector<std::unordered_set<std::string>> batches_keys;
    {
      std::lock_guard l(command_queue_mutex_);
      while (!commands_queue_.empty()) {
        const std::string& next_key = commands_queue_.front().first;

        if (next_key.empty()) {
          if (batches.empty()) {
            // Nothing staged yet: flush it right now as a single-command batch.
            batches.emplace_back(1, commands_queue_.front());
            batches_keys.emplace_back();
            elements_++;
            commands_queue_.pop();
          }
          // Otherwise leave it at the queue front; the next pass sends it alone,
          // after this window has been acknowledged.
          break;
        }

        // Choose the first batch, in index order, that still has room and does
        // not hold this key yet.
        size_t idx = batches.size();
        for (size_t i = 0; i < batches.size(); ++i) {
          if (batches[i].size() < pipeline_size
              && batches_keys[i].find(next_key) == batches_keys[i].end()) {
            idx = i;
            break;
          }
        }

        if (idx == batches.size()) {
          if (batches.size() >= pipeline_window) {
            // Window exhausted: every batch is either full or already holds
            // this key, so the rest of the queue waits for the next pass.
            break;
          }
          batches.emplace_back();
          batches_keys.emplace_back();
        }

        batches_keys[idx].insert(next_key);
        batches[idx].push_back(commands_queue_.front());
        elements_++;
        commands_queue_.pop();
      }
    }

    wsignal_.notify_all();
    for (size_t i = 0; i < batches.size(); ++i) {
      ret = SendCommands(batches[i]);
      window_passes_total++;
      if (ret != 0) {
        failed_passes_total++;
        LOG(WARNING) << "RedisSender " << id_ << " SendCommands failed, target: " << ip_ << ":" << port_
                     << ", batch: " << i << "/" << batches.size() << ", batch size: " << batches[i].size()
                     << ", these commands may not have reached the target";
      } else {
        replies_received_total += batches[i].size();
      }
    }

    const auto now_us = pstd::NowMicros();
    if (now_us - last_throughput_log_us >= kThroughputLogPeriodUs) {
      const double period_s = (now_us - last_throughput_log_us) / 1000000.0;
      const double total_s = (now_us - thread_start_us) / 1000000.0;
      const int64_t period_replies = replies_received_total - last_logged_replies;
      LOG(INFO) << "RedisSender throughput, id: " << id_
                << ", period: " << period_s << " s, replies received: " << period_replies
                << " (" << (period_s > 0 ? period_replies / period_s : 0.0) << " replies/s)"
                << ", total replies received: " << replies_received_total
                << " (" << (total_s > 0 ? replies_received_total / total_s : 0.0) << " replies/s avg)"
                << ", queue depth: " << commandQueueSize()
                << ", window passes: " << window_passes_total
                << " (failed: " << failed_passes_total << ")"
                << ", avg cmds per pass: "
                << (window_passes_total > 0 ? replies_received_total / window_passes_total : 0);
      last_throughput_log_us = now_us;
      last_logged_replies = replies_received_total;
    }
  }

  // On exit, warn if there are still un-sent commands in the queue.
  //  - forced stop: expected, the queue is intentionally abandoned.
  //  - graceful stop: should be 0 here; a non-zero count means the drain loop
  //    exited early and is a real bug worth surfacing.
  {
    size_t remained = commandQueueSize();
    if (remained > 0) {
      if (graceful_exit_) {
        LOG(WARNING) << "RedisSender " << id_ << " graceful-exit but " << remained
                     << " commands still queued (drain incomplete), target: " << ip_ << ":" << port_
                     << ", these commands will be DROPPED";
      } else {
        LOG(WARNING) << "RedisSender " << id_ << " forced-exit with " << remained
                     << " un-sent commands still in queue, target: " << ip_ << ":" << port_
                     << ", these commands will be DROPPED";
      }
    }
  }

  const auto thread_end_us = pstd::NowMicros();
  const double total_s = (thread_end_us - thread_start_us) / 1000000.0;
  LOG(INFO) << "RedisSender thread " << id_ << " complete, target: " << ip_ << ":" << port_
            << ", dequeued/staged: " << elements_ << " cmds, replies received: " << replies_received_total
            << " in " << total_s << " s"
            << ", avg reply rate: " << (total_s > 0 ? replies_received_total / total_s : 0.0) << " replies/s"
            << ", window passes: " << window_passes_total << " (failed: " << failed_passes_total << ")"
            << ", avg cmds per pass: "
            << (window_passes_total > 0 ? replies_received_total / window_passes_total : 0);
  cli_ = NULL;
  return NULL;
}