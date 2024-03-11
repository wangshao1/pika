#include <thread>
#include <iostream>
#include <queue>
#include <atomic>
#include <gtest/gtest.h>
#include "glog/logging.h"

#include "pstd/include/env.h"
#include "storage/storage.h"
#include "src/redis.h"
#include "storage/util.h"

using namespace storage;

std::queue<std::pair<std::string, rocksdb::ReplicationLogRecord>> items;

struct MockReplicationListener : public rocksdb::ReplicationLogListener{
  MockReplicationListener() = default;
  ~MockReplicationListener() = default;
  std::string OnReplicationLogRecord(rocksdb::ReplicationLogRecord record) override {
    std::string cnt = std::to_string(counter_.fetch_add(1));
    items.push(std::make_pair(cnt, record));
    LOG(WARNING) << "write binlog, replication_sequence: " << cnt << " type: " << record.type << " items count:" << items.size();
    return cnt;
  }
  std::atomic<int> counter_ = {0};
};

class CloudTest : public ::testing::Test {
public:
  CloudTest() = default;
  ~CloudTest() override = default;

  void SetUp() override {
    storage_options.options.create_if_missing = true;
    storage_options.options.avoid_flush_during_shutdown = true;
    auto& cloud_fs_opts = storage_options.cloud_fs_options;
    cloud_fs_opts.endpoint_override = "http://127.0.0.1:9000";
    cloud_fs_opts.credentials.InitializeSimple("minioadmin", "minioadmin");
    ASSERT_TRUE(cloud_fs_opts.credentials.HasValid().ok());
    cloud_fs_opts.src_bucket.SetBucketName("database.unit.test", "pika.");
    cloud_fs_opts.dest_bucket.SetBucketName("database.unit.test", "pika.");
    storage_options.options.max_log_file_size = 0;
  }

  void TearDown() override {
  }

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  StorageOptions storage_options;
  storage::Status s;
  std::string path;
};

Status OpenMaster(storage::Redis*& inst, StorageOptions storage_options) {
  storage::Storage str;
  while (!items.empty())
  {
    items.pop();
  }

  inst = new storage::Redis(&str, 0);
  auto listener = std::make_shared<MockReplicationListener>();
  inst->ResetListener(listener);
  storage_options.cloud_fs_options.is_master = true;
  auto s = inst->Open(storage_options, "cloud_test");
  return s;
}

Status OpenSlave(storage::Redis*& inst, StorageOptions storage_options) {
  storage::Storage str;
  inst = new storage::Redis(&str, 0);
  storage_options.cloud_fs_options.is_master = false;
  auto s = inst->Open(storage_options, "cloud_test");
  return s;
}

TEST_F(CloudTest, simple_master) {
  storage::Redis* inst;
  auto s = OpenMaster(inst, storage_options);
  ASSERT_TRUE(s.ok());
  for (int i = 0; i < 10000; i++) {
      if (i + 1 % 100 == 0) {
          sleep(1);
      }
      s = inst->Set(std::to_string(i), std::to_string(i));
      ASSERT_TRUE(s.ok());
  }
  rocksdb::FlushOptions fo;
  fo.wait = true;
  inst->GetDB()->Flush(fo);
  delete inst;
  inst = nullptr;
}

Status SlaveCatchUp(storage::Redis* slave) {
  Status s;
  LOG(WARNING) << "SlaveCatchUp, items.size: " << items.size();
  while (!items.empty()) {
    std::string replication_sequence = items.front().first;
    auto record = items.front().second;
    items.pop();
    LOG(WARNING) << "replication_sequence: " << replication_sequence << " type: " << record.type;
    // slave catchup
    rocksdb::DB::ApplyReplicationLogRecordInfo info;
    s = slave->GetDB()->ApplyReplicationLogRecord(record, replication_sequence, nullptr, true, &info,  rocksdb::DB::AR_EVICT_OBSOLETE_FILES);
    if (!s.ok()) {
      LOG(WARNING) << "reapply log error: " << s.ToString();
        return s;
    }
  }
  return s;
}

TEST_F(CloudTest, master_slave) {
  storage::Redis* inst_master, *inst_slave;
  auto s = OpenMaster(inst_master, storage_options);
  ASSERT_TRUE(s.ok());
  // master write
  for (int i = 0; i < 20000; i++) {
    if (i + 1 % 100 == 0) {
      sleep(1);
    }
    s = inst_master->Set(std::to_string(i), std::to_string(i));
    ASSERT_TRUE(s.ok());
  }

  rocksdb::FlushOptions fo;
  fo.wait = true;
  inst_master->GetDB()->Flush(fo);
  delete inst_master;
  inst_master = nullptr;

  std::vector<std::string> children;
  pstd::GetChildren("cloud_test", children);
  std::for_each(children.begin(), children.end(), [](auto& file) {
      if (file.find("sst") != std::string::npos) {
        std::string path = "cloud_test/";
        path = path + file;
        pstd::DeleteFile(path);
      }
  });

  s = OpenSlave(inst_slave, storage_options);
  ASSERT_TRUE(s.ok());
  for (int i = 0; i < 20000; i++) {
    std::string val;
    s = inst_slave->Get(std::to_string(i), &val);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(val, std::to_string(i));
  }
  SlaveCatchUp(inst_slave);

  delete inst_slave;
  inst_slave = nullptr;

  s = OpenMaster(inst_master, storage_options);
  ASSERT_TRUE(s.ok());
  for (int i = 0; i < 20000; i++) {
    std::string val;
    s = inst_master->Get(std::to_string(i), &val);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(val, std::to_string(i));
  }
  delete inst_master;
  inst_master = nullptr;
}

TEST_F(CloudTest, switch_master) {
  storage::Redis* inst_master, *inst_slave;
  auto s = OpenMaster(inst_master, storage_options);
  ASSERT_TRUE(s.ok());
  // master write
  for (int i = 0; i < 20000; i++) {
    if (i + 1 % 100 == 0) {
      sleep(1);
    }
    s = inst_master->Set(std::to_string(i), std::to_string(i));
    ASSERT_TRUE(s.ok());
  }

  delete inst_master;
  inst_master = nullptr;
  LOG(WARNING) << "close master already";
  sleep(20);

  std::vector<std::string> children;
  pstd::GetChildren("cloud_test", children);
  std::for_each(children.begin(), children.end(), [](auto& file) {
      if (file.find("sst") != std::string::npos) {
        std::string path = "cloud_test/";
        path = path + file;
        pstd::DeleteFile(path);
      }
  });

  s = OpenSlave(inst_slave, storage_options);
  ASSERT_TRUE(s.ok());
  s = SlaveCatchUp(inst_slave);
  ASSERT_TRUE(s.ok());
  for (int i = 0; i < 20000; i++) {
    std::string val;
    s = inst_slave->Get(std::to_string(i), &val);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(val, std::to_string(i));
  }
  s = inst_slave->SwitchMaster(false, true);
  ASSERT_TRUE(s.ok());
  delete inst_slave;
  inst_slave = nullptr;

  pstd::GetChildren("cloud_test", children);
  std::for_each(children.begin(), children.end(), [](auto& file) {
      if (file.find("sst") != std::string::npos) {
        std::string path = "cloud_test/";
        path = path + file;
        pstd::DeleteFile(path);
      }
  });

  s = OpenMaster(inst_master, storage_options);
  ASSERT_TRUE(s.ok());
  for (int i = 0; i < 20000; i++) {
    std::string val;
    s = inst_master->Get(std::to_string(i), &val);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(val, std::to_string(i));
  }
  delete inst_master;
  inst_master = nullptr;
}

int main(int argc, char** argv) {
  if (!pstd::FileExists("./log")) {
    pstd::CreatePath("./log");
  }
  FLAGS_log_dir = "./log";
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("cloud_test");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
