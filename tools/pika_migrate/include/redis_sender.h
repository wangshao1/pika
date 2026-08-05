#ifndef REDIS_SENDER_H_
#define REDIS_SENDER_H_

#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <queue>
#include <vector>
#include <utility>
#include <unordered_set>
#include <mutex>
#include <condition_variable>

#include "net/include/net_thread.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

class RedisSender : public net::Thread {
 public:
  RedisSender(int id, std::string ip, int64_t port, std::string user, std::string password);
  virtual ~RedisSender();
  // Forced stop: exit as soon as possible, ABANDONING any commands still queued.
  // Use only when correctness of the queued data no longer matters (e.g. process
  // teardown in ~PikaServer()).
  void Stop(void);
  // Graceful stop: keep sending until the queue is fully drained, THEN exit.
  // Use at the end of a migration once all producers have finished, so no
  // enqueued command is dropped (a source of src/dst divergence).
  void GracefulStop(void);
  int64_t elements() {
    return elements_;
  }

  // key is used to guarantee ordering under a codis-like target: within one
  // pipeline batch the same key never appears twice (see ThreadMain), so the
  // proxy's multi-connection fan-out to backends cannot reorder writes of the
  // same key. Pass an empty key only when the command is not key-scoped.
  void SendRedisCommand(const std::string &key, const std::string &command);

 private:
  // key is carried only for logging (which key/command failed); command is the
  // serialized RESP bytes actually sent.
  int SendCommand(const std::string &key, std::string &command);
  // Pipeline: send a batch of commands then read all replies. On failure it
  // reconnects and resends the batch one-by-one to preserve ordering.
  // Each entry is (key, serialized command); key is used only for logging.
  int SendCommands(std::vector<std::pair<std::string, std::string>> &commands);
  void ConnectRedis();
  size_t commandQueueSize() {
    std::lock_guard l(command_queue_mutex_);
    return commands_queue_.size();
  }
  virtual void *ThreadMain();
 private:
  int id_;
  int port_;
  std::shared_ptr<net::NetCli> cli_;
  std::condition_variable rsignal_;
  std::condition_variable wsignal_;
  std::mutex signal_mutex_;
  std::mutex command_queue_mutex_;
  // Each entry is (key, serialized command). key drives per-batch dedup.
  std::queue<std::pair<std::string, std::string>> commands_queue_;
  std::string ip_;
  std::string user_;
  std::string password_;
  // should_exit_ requests the thread to stop. When graceful_exit_ is also set,
  // the thread first drains commands_queue_ before leaving; otherwise it leaves
  // immediately and any queued commands are dropped.
  bool should_exit_;
  std::atomic<bool> graceful_exit_{false};
  int64_t elements_;
  std::atomic<time_t> last_write_time_;
};

#endif
