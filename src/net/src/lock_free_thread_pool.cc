// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sys/time.h>
#include <utility>

#include "glog/logging.h"

#include "net/include/lock_free_thread_pool.h"
#include "net/src/net_thread_name.h"

namespace net {

void* LockFreeThreadPool::Worker::WorkerMain(void* arg) {
  auto tp = static_cast<LockFreeThreadPool*>(arg);
  tp->runInThread();
  return nullptr;
}

int LockFreeThreadPool::Worker::start() {
  if (!start_.load()) {
    if (pthread_create(&thread_id_, nullptr, &WorkerMain, thread_pool_) != 0) {
      return -1;
    } else {
      start_.store(true);
      SetThreadName(thread_id_, thread_pool_->thread_pool_name() + "Worker");
    }
  }
  return 0;
}

int LockFreeThreadPool::Worker::stop() {
  if (start_.load()) {
    if (pthread_join(thread_id_, nullptr) != 0) {
      return -1;
    } else {
      start_.store(false);
    }
  }
  return 0;
}

LockFreeThreadPool::LockFreeThreadPool(size_t worker_num, size_t max_queue_size, const std::string& thread_pool_name)
    : worker_num_(worker_num),
      max_queue_size_(max_queue_size),
      thread_pool_name_(std::move(thread_pool_name)),
      running_(false),
      queue_(max_queue_size_),
      should_stop_(false) {}

LockFreeThreadPool::~LockFreeThreadPool() { stop_thread_pool(); }

int LockFreeThreadPool::start_thread_pool() {
  if (!running_.load()) {
    should_stop_.store(false);
    for (size_t i = 0; i < worker_num_; ++i) {
      workers_.push_back(new Worker(this));
      int res = workers_[i]->start();
      if (res != 0) {
        return kCreateThreadError;
      }
    }
    running_.store(true);
  }
  return kSuccess;
}

int LockFreeThreadPool::stop_thread_pool() {
  int res = 0;
  if (running_.load()) {
    should_stop_.store(true);
    rsignal_.notify_all();
    wsignal_.notify_all();
    for (const auto worker : workers_) {
      res = worker->stop();
      if (res != 0) {
        break;
      } else {
        delete worker;
      }
    }
    workers_.clear();
    running_.store(false);
  }
  return res;
}

bool LockFreeThreadPool::should_stop() { return should_stop_.load(); }

void LockFreeThreadPool::set_should_stop() { should_stop_.store(true); }

void LockFreeThreadPool::Schedule(TaskFunc func, void* arg) {
  Task task{func, arg};
  int retry_cnt = 0;
  bool success = false;
  thread_local int print_cnt = 0;
  if (print_cnt++ % 1000 == 0) {
    LOG(WARNING) << "queue size: " << queue_.size_approx();
  }
  while (retry_cnt++ < 100 && !success) {
    success = queue_.try_enqueue(task);
    if (success) {
      auto now = std::chrono::system_clock::now();
      uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
      /*
      if (unow - task.ts > 1000 * 10) {
        LOG(WARNING) << "before in queue for: " << (unow - task.ts) / 1000 << " ms";
      }
      */
      rsignal_.notify_one();
      return;
    }
    LOG(WARNING) << "retry_cnt: " << retry_cnt;
  }

  std::unique_lock<std::mutex> lock(mu_);
  wsignal_.wait(lock, [this, &task]() { return queue_.try_enqueue(task) || should_stop(); });

  if (!should_stop()) {
    rsignal_.notify_one();
  }
}

size_t LockFreeThreadPool::max_queue_size() { return max_queue_size_; }

void LockFreeThreadPool::cur_queue_size(size_t* qsize) {
  *qsize = queue_.size_approx();
}

std::string LockFreeThreadPool::thread_pool_name() { return thread_pool_name_; }

void LockFreeThreadPool::runInThread() {
  while (!should_stop()) {
    bool success = false;
    int retry_cnt = 0;
    Task task;
    /*
    auto now1 = std::chrono::system_clock::now();
    uint64_t unow1 = std::chrono::duration_cast<std::chrono::microseconds>(now1.time_since_epoch()).count();
    */
    while (retry_cnt++ < 3 && !success) {
      success = queue_.try_dequeue(task);
      /*
      auto now = std::chrono::system_clock::now();
      uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
      if (unow - task.ts > 1000 * 10) {
        LOG(WARNING) << "waiting in queue for: " << (unow - task.ts) / 1000 << " ms" << " wait:" << (unow - unow1) << "us";
      }
      */
      wsignal_.notify_one();
      if (success) {
        break;
      }
    }
    if (!success) {
      std::unique_lock<std::mutex> lock(mu_);
      rsignal_.wait(lock, [this, &task]() { return queue_.try_dequeue(task) || should_stop(); });
      if (should_stop()) {
        break;
      }
      /*
      auto now = std::chrono::system_clock::now();
      uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
      if (unow - task.ts > 1000 * 10) {
        LOG(WARNING) << "waiting in queue for: " << (unow - task.ts) / 1000 << " ms";
      }
      */
      wsignal_.notify_one();
    }

    /*
    auto now = std::chrono::system_clock::now();
    uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    if (unow - task.ts > 1000 * 10) {
      LOG(WARNING) << "waiting in queue for: " << (unow - task.ts) / 1000 << " ms";
    }
    */

    (*task.func)(task.arg);
  }
}
}  // namespace net
