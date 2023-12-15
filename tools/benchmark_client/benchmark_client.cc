#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <functional>
#include <iostream>
#include <random>
#include <vector>

#include "gflags/gflags.h"
#include "monitoring/histogram.h"
#include "hiredis/hiredis.h"

#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/env.h"

DEFINE_string(command, "generate", "command to execute, eg: generate/get/set/zadd");
DEFINE_string(host, "127.0.0.1", "target server's host");
DEFINE_int32(port, 9221, "target server's listen port");
DEFINE_int32(timeout, 1000, "request timeout");
DEFINE_bool(pipeline, false, "whether to enable pipeline");
DEFINE_string(password, "", "password");
DEFINE_int32(key_size, 50, "key size int bytes");
DEFINE_int32(value_size, 100, "value size in bytes");
DEFINE_int32(count, 100000, "request counts");
DEFINE_int32(thread_num, 10, "concurrent thread num");
DEFINE_string(dbs, "0", "dbs name, eg: 0,1,2");
DEFINE_int32(element_count, 1, "elements number in hash/list/set/zset");

#define TIME_OF_LOOP 1000000

using pstd::Status;
using std::default_random_engine;

struct RequestStat {
  int success_cnt = 0;
  int timeout_cnt = 0;
  int error_cnt = 0;
  RequestStat operator+(const RequestStat& stat) {
    RequestStat res;
    res.success_cnt = this->success_cnt + stat.success_cnt;
    res.timeout_cnt = this->timeout_cnt + stat.timeout_cnt;
    res.error_cnt = this->error_cnt + stat.error_cnt;
    return res;
  }
};

struct ThreadArg {
  ThreadArg(pthread_t t, const std::string& tn, int i)
      : idx(i), tid(t), table_name(tn), stat() {}
  int idx;
  pthread_t tid;
  std::string table_name;
  RequestStat stat;
};

Status RunHGetAllCommand(redisContext* c, ThreadArg* arg);
Status RunGetCommand(redisContext* c, ThreadArg* arg);
Status RunSetCommand(redisContext* c, ThreadArg* arg);
Status RunHSetCommand(redisContext* c, ThreadArg* arg);
Status RunSetCommandPipeline(redisContext* c);
Status RunZAddCommand(redisContext* c);

std::vector<ThreadArg> thread_args;
thread_local int last_seed = 0;
std::vector<std::string> tables;
rocksdb::HistogramImpl hist;
int pipeline_num = 0;

void GenerateRandomString(int32_t len, std::string* target) {
  target->clear();
  char c_map[67] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'g', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
                    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                    'I', 'G', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
                    'Z', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '=', '_', '+'};

  default_random_engine e;
  for (int i = 0; i < len; i++) {
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 67;
    target->push_back(c_map[rand_num]);
  }
}

void PrintInfo(const std::time_t& now) {
  std::cout << "=================== Benchmark Client ===================" << std::endl;
  std::cout << "Server host name: " << FLAGS_host << std::endl;
  std::cout << "Server port: " << FLAGS_port << std::endl;
  std::cout << "command: " << FLAGS_command << std::endl;
  std::cout << "Thread num : " << FLAGS_thread_num << std::endl;
  std::cout << "Payload size : " << FLAGS_value_size << std::endl;
  std::cout << "Number of request : " << FLAGS_count << std::endl;
  std::cout << "Transmit mode: " << (FLAGS_pipeline ? "Pipeline" : "No Pipeline") << std::endl;
  std::cout << "Collection of dbs: " << FLAGS_dbs << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now)) << std::endl;
  std::cout << "Elements num: " << FLAGS_element_count;
  std::cout << "========================================================" << std::endl;
}

void RunGenerateCommand(int index) {
  std::string filename = "benchmark_keyfile_" + std::to_string(index);
  FILE* fp = fopen(filename.c_str(), "w+");
  if (fp == nullptr) {
    std::cout << "open file error";
    return;
  }
  for (size_t i = 0; i < FLAGS_count * FLAGS_element_count; i++) {
    std::string key;
    GenerateRandomString(FLAGS_key_size, &key);
    key.append("\n");
    fwrite(key.data(), sizeof(char), key.size(), fp);
  }
  fclose(fp);
}

redisContext* Prepare(ThreadArg* arg) {
  int index = arg->idx;
  std::string table = arg->table_name;
  struct timeval timeout = {FLAGS_timeout / 1000, (FLAGS_timeout % 1000) * 1000};
  redisContext* c = redisConnectWithTimeout(FLAGS_host.data(), FLAGS_port, timeout);
  if (!c || c->err) {
    if (c) {
      printf("Table: %s Thread %d, Connection error: %s\n", table.c_str(), index, c->errstr);
      redisFree(c);
    } else {
      printf("Table %s Thread %d, Connection error: can't allocate redis context\n", table.c_str(), index);
    }
    return nullptr;
  }

  if (!FLAGS_password.empty()) {
    const char* auth_argv[2] = {"AUTH", FLAGS_password.data()};
    size_t auth_argv_len[2] = {4, FLAGS_password.size()};
    auto res = reinterpret_cast<redisReply*>(redisCommandArgv(c, 2, reinterpret_cast<const char**>(auth_argv),
                                                              reinterpret_cast<const size_t*>(auth_argv_len)));
    if (!res) {
      printf("Table %s Thread %d Auth Failed: Get reply Error\n", table.c_str(), index);
      freeReplyObject(res);
      redisFree(c);
      return nullptr;
    } else {
      if (!strcasecmp(res->str, "OK")) {
      } else {
        printf("Table %s Thread %d Auth Failed: %s\n", table.c_str(), index, res->str);
        freeReplyObject(res);
        redisFree(c);
        return nullptr;
      }
    }
    freeReplyObject(res);
  }

  const char* select_argv[2] = {"SELECT", arg->table_name.data()};
  size_t select_argv_len[2] = {6, arg->table_name.size()};
  auto res = reinterpret_cast<redisReply*>(redisCommandArgv(c, 2, reinterpret_cast<const char**>(select_argv),
                                                            reinterpret_cast<const size_t*>(select_argv_len)));
  if (!res) {
    printf("Thread %d Select Table %s Failed, Get reply Error\n", index, table.c_str());
    freeReplyObject(res);
    redisFree(c);
    return nullptr;
  } else {
    if (!strcasecmp(res->str, "OK")) {
      printf("Table %s Thread %d Select DB Success, start to write data...\n", table.c_str(), index);
    } else {
      printf("Table %s Thread %d Select DB Failed: %s, thread exit...\n", table.c_str(), index, res->str);
      freeReplyObject(res);
      redisFree(c);
      return nullptr;
    }
  }
  return c;
}

void* ThreadMain(void* arg) {
  ThreadArg* ta = reinterpret_cast<ThreadArg*>(arg);
  last_seed = (int)pthread_self();

  if (FLAGS_command == "generate") {
    RunGenerateCommand(ta->idx);
    return nullptr;
  }

  redisContext* c = Prepare(ta);
  if (!c) {
    return nullptr;
  }

  Status s;
  if (FLAGS_command == "get") {
    s = RunGetCommand(c, ta);
  } else if (FLAGS_command == "set") {
    if (!FLAGS_pipeline) {
      s = RunSetCommand(c, ta);
    } else {
      s = RunSetCommandPipeline(c);
    }
  } else if (FLAGS_command == "hset") {
    s = RunHSetCommand(c, ta);
  } else if (FLAGS_command == "hgetall") {
    s = RunHGetAllCommand(c,ta);
  }

  if (!s.ok()) {
    std::string thread_info = "Table " + ta->table_name + ", Thread " + std::to_string(ta->idx);
    printf("%s, %s, thread exit...\n", thread_info.c_str(), s.ToString().c_str());
  }
  redisFree(c);
  return nullptr;
}

Status RunSetCommandPipeline(redisContext* c) {
  redisReply* res = nullptr;
  for (size_t idx = 0; idx < FLAGS_count; (idx += pipeline_num)) {
    const char* argv[3] = {"SET", nullptr, nullptr};
    size_t argv_len[3] = {3, 0, 0};
    for (int32_t batch_idx = 0; batch_idx < pipeline_num; ++batch_idx) {
      std::string key;
      std::string value;

      default_random_engine e;
      e.seed(last_seed);
      last_seed = e();
      int32_t rand_num = last_seed % 10 + 1;
      GenerateRandomString(rand_num, &key);
      GenerateRandomString(FLAGS_value_size, &value);

      argv[1] = key.c_str();
      argv_len[1] = key.size();
      argv[2] = value.c_str();
      argv_len[2] = value.size();

      if (redisAppendCommandArgv(c, 3, reinterpret_cast<const char**>(argv),
                                 reinterpret_cast<const size_t*>(argv_len)) == REDIS_ERR) {
        return Status::Corruption("Redis Append Command Argv Error");
      }
    }

    for (int32_t batch_idx = 0; batch_idx < pipeline_num; ++batch_idx) {
      if (redisGetReply(c, reinterpret_cast<void**>(&res)) == REDIS_ERR) {
        return Status::Corruption("Redis Pipeline Get Reply Error");
      } else {
        if (!res || strcasecmp(res->str, "OK")) {
          std::string res_str = "Exec command error: " + (res != nullptr ? std::string(res->str) : "");
          freeReplyObject(res);
          return Status::Corruption(res_str);
        }
        freeReplyObject(res);
      }
    }
  }
  return Status::OK();
}

Status RunGetCommand(redisContext* c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::string> keys(FLAGS_count, "");
  std::string filename = "benchmark_keyfile_" + std::to_string(arg->idx);
  FILE* fp = fopen(filename.c_str(), "r");
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, 1000, fp);
    key[FLAGS_key_size] = '\0';
    keys[idx] = std::string(key);
  }
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    const char* set_argv[2];
    size_t set_argvlen[2];
    std::string key;
    std::string value;
    default_random_engine e;
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 10 + 1;

    key = keys[idx];

    set_argv[0] = "get";
    set_argvlen[0] = 3;
    set_argv[1] = key.c_str();
    set_argvlen[1] = key.size();

    uint64_t begin = pstd::NowMicros();

    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 3, reinterpret_cast<const char**>(set_argv), reinterpret_cast<const size_t*>(set_argvlen)));
    uint64_t cost = pstd::NowMicros() - begin;
    hist.Add(cost);
    if (!res) {
      std::string res_str = "Exec command error: " + (res != nullptr ? std::string(res->str) : "");
      std::cout << res_str << std::endl;
      arg->stat.error_cnt++;
    } else {
      arg->stat.success_cnt++;
    }
    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunHGetAllCommand(redisContext* c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::vector<std::string>>> keys(FLAGS_count);
  std::string filename = "benchmark_keyfile_" + std::to_string(arg->idx);
  FILE* fp = fopen(filename.c_str(), "r");
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, 1000, fp);
    key[FLAGS_key_size] = '\0';
    std::vector<std::string> elements;
    for (size_t idy = 0; idy < FLAGS_element_count; ++idy) {
      char* element = new char[FLAGS_key_size + 2];
      fgets(element, 1000, fp);
      element[FLAGS_key_size] = '\0';
      elements.push_back(std::string(element));
    }
    keys[idx] = std::make_pair(std::string(key), elements);
  }
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    const char* set_argv[2];
    size_t set_argvlen[2];
    std::string pkey, element;
    std::string value;
    default_random_engine e;
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 10 + 1;

    //GenerateRandomString(rand_num, &key);
    pkey = keys[idx].first;

    set_argv[0] = "hgetall";
    set_argvlen[0] = 7;
    set_argv[1] = pkey.c_str();
    set_argvlen[1] = pkey.size();

    uint64_t begin = pstd::NowMicros();
    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 2, reinterpret_cast<const char**>(set_argv), reinterpret_cast<const size_t*>(set_argvlen)));
    uint64_t cost = pstd::NowMicros() - begin;
    hist.Add(cost);
    if (!res || strcasecmp(res->str, "OK")) {
      std::string res_str = "Exec hgetall error: " + (res != nullptr ? std::string(res->str) : "");
      std::cout << res_str << std::endl;
      arg->stat.error_cnt++;
    } else {
      arg->stat.success_cnt++;
    }
    //TODO: elements check
    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunHSetCommand(redisContext* c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::pair<std::string, std::vector<std::string>>> keys(FLAGS_count);
  std::string filename = "benchmark_keyfile_" + std::to_string(arg->idx);
  FILE* fp = fopen(filename.c_str(), "r");
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, 1000, fp);
    key[FLAGS_key_size] = '\0';
    std::vector<std::string> elements;
    elements.push_back(std::string(key));
    for (size_t idy = 1; idy < FLAGS_element_count; ++idy) {
      char* element = new char[FLAGS_key_size + 2];
      fgets(element, 1000, fp);
      element[FLAGS_key_size] = '\0';
      elements.push_back(std::string(element));
    }
    keys[idx] = std::make_pair(std::string(key), elements);
  }
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    const char* set_argv[4];
    size_t set_argvlen[4];
    std::string pkey;
    std::string value;
    default_random_engine e;
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 10 + 1;

    //GenerateRandomString(rand_num, &key);
    pkey = keys[idx].first;
    for (const auto& member : keys[idx].second) {
      GenerateRandomString(FLAGS_value_size, &value);
      set_argv[0] = "hset";
      set_argvlen[0] = 4;
      set_argv[1] = pkey.c_str();
      set_argvlen[1] = pkey.size();
      set_argv[2] = member.c_str();
      set_argvlen[2] = member.size();
      set_argv[3] = value.c_str();
      set_argvlen[3] = value.size();

      uint64_t begin = pstd::NowMicros();
      res = reinterpret_cast<redisReply*>(
          redisCommandArgv(c, 4, reinterpret_cast<const char**>(set_argv), reinterpret_cast<const size_t*>(set_argvlen)));
      uint64_t cost = pstd::NowMicros() - begin;
      hist.Add(cost);
      if (!res || strcasecmp(res->str, "OK")) {
        std::string res_str = "Exec command error: " + (res != nullptr ? std::string(res->str) : "");
        std::cout << res_str << std::endl;
        arg->stat.error_cnt++;
      } else {
        arg->stat.success_cnt++;
      }
      freeReplyObject(res);
    }
  }
  return Status::OK();
}

Status RunSetCommand(redisContext* c, ThreadArg* arg) {
  redisReply* res = nullptr;
  std::vector<std::string> keys(FLAGS_count, "");
  std::string filename = "benchmark_keyfile_" + std::to_string(arg->idx);
  FILE* fp = fopen(filename.c_str(), "r");
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    char* key = new char[FLAGS_key_size + 2];
    fgets(key, 1000, fp);
    key[FLAGS_key_size] = '\0';
    keys[idx] = std::string(key);
  }
  for (size_t idx = 0; idx < FLAGS_count; ++idx) {
    const char* set_argv[3];
    size_t set_argvlen[3];
    std::string key;
    std::string value;
    default_random_engine e;
    e.seed(last_seed);
    last_seed = e();
    int32_t rand_num = last_seed % 10 + 1;

    //GenerateRandomString(rand_num, &key);
    key = keys[idx];
    GenerateRandomString(FLAGS_value_size, &value);

    set_argv[0] = "set";
    set_argvlen[0] = 3;
    set_argv[1] = key.c_str();
    set_argvlen[1] = key.size();
    set_argv[2] = value.c_str();
    set_argvlen[2] = value.size();

    uint64_t begin = pstd::NowMicros();

    res = reinterpret_cast<redisReply*>(
        redisCommandArgv(c, 3, reinterpret_cast<const char**>(set_argv), reinterpret_cast<const size_t*>(set_argvlen)));
    uint64_t cost = pstd::NowMicros() - begin;
    hist.Add(cost);
    if (!res || strcasecmp(res->str, "OK")) {
      std::string res_str = "Exec command error: " + (res != nullptr ? std::string(res->str) : "");
      std::cout << res_str << std::endl;
    }
    freeReplyObject(res);
  }
  return Status::OK();
}

Status RunZAddCommand(redisContext* c) {
  redisReply* res = nullptr;
  for (size_t idx = 0; idx < 1; ++idx) {
    const char* zadd_argv[4];
    size_t zadd_argvlen[4];
    std::string key;
    std::string score;
    std::string member;
    GenerateRandomString(10, &key);

    zadd_argv[0] = "zadd";
    zadd_argvlen[0] = 4;
    zadd_argv[1] = key.c_str();
    zadd_argvlen[1] = key.size();
    for (size_t sidx = 0; sidx < 10000; ++sidx) {
      score = std::to_string(sidx * 2);
      GenerateRandomString(FLAGS_key_size, &member);
      zadd_argv[2] = score.c_str();
      zadd_argvlen[2] = score.size();
      zadd_argv[3] = member.c_str();
      zadd_argvlen[3] = member.size();

      res = reinterpret_cast<redisReply*>(redisCommandArgv(c, 4, reinterpret_cast<const char**>(zadd_argv),
                                                           reinterpret_cast<const size_t*>(zadd_argvlen)));
      if (!res || !res->integer) {
        std::string res_str = "Exec command error: " + (res != nullptr ? std::string(res->str) : "");
        freeReplyObject(res);
        return Status::Corruption(res_str);
      }
      freeReplyObject(res);
    }
  }
  return Status::OK();
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  pstd::StringSplit(FLAGS_dbs, ',', tables);
  if (tables.empty()) {
    exit(-1);
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  for (const auto& table : tables) {
    for (size_t idx = 0; idx < FLAGS_thread_num; ++idx) {
      thread_args.push_back({0, table, idx});
    }
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_create(&thread_args[idx].tid, nullptr, ThreadMain, &thread_args[idx]);
  }

  for (size_t idx = 0; idx < thread_args.size(); ++idx) {
    pthread_join(thread_args[idx].tid, nullptr);
  }

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();


  std::cout << "Total Time Cost : " << hours << " hours " << minutes % 60 << " minutes " << seconds % 60 << " seconds "
            << std::endl;
  std::cout << "stats: " << hist.ToString() << std::endl;
  return 0;
}
