//
// Created by Bai Xin on 2024/3/11.
//
#include <gtest/gtest.h>

#include <iostream>

#include "../../include/pika_binlog_reader.h"
#include "../../include/pika_cloud_binlog.h"
#include "include/pika_cloud_binlog_transverter.h"

class CloudBinlogTransverterTest : public ::testing::Test {};

class CloudBinlogTest : public ::testing::Test {
 public:
  CloudBinlogTest() = default;
  ~CloudBinlogTest() override = default;

  void SetUp() override {
    std::string path = "./cloudbinlog/";
    pstd::DeleteDirIfExist(path);
    mkdir(path.c_str(), 0755);
    cloudBinlog = std::make_shared<CloudBinlog>(path);
  }

  void TearDown() override {
    std::string path = "./cloudbinlog";
    pstd::DeleteFile(path.c_str());
  }

  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  std::shared_ptr<Binlog> cloudBinlog;
};

TEST_F(CloudBinlogTest, GetPutTest) {
  pstd::Status s = CloudBinlogTest::cloudBinlog->Put("test", 1, 1);
  ASSERT_TRUE(s.ok());

  PikaBinlogReader binlog_reader;
  uint32_t filenum = 0;
  uint32_t term = 0;
  uint64_t offset = 0;

  s = CloudBinlogTest::cloudBinlog->GetProducerStatus(&filenum, &offset, &term, nullptr);
  ASSERT_TRUE(s.ok());

  s = CloudBinlogTest::cloudBinlog->Put("yyyy", 1, 1);
  ASSERT_TRUE(s.ok());

  int res = binlog_reader.Seek(CloudBinlogTest::cloudBinlog, filenum, offset);
  ASSERT_EQ(res, 0);

  std::string binlog;
  s = binlog_reader.Get(&binlog, &filenum, &offset);
  ASSERT_TRUE(s.ok());

  cloud::BinlogCloudItem* binlog_item = new cloud::BinlogCloudItem();
  PikaCloudBinlogTransverter::BinlogDecode(binlog, binlog_item);
  ASSERT_EQ(1, binlog_item->db_id());
  ASSERT_EQ(1, binlog_item->rocksdb_id());
  ASSERT_STREQ("yyyy", binlog_item->content().c_str());

  delete binlog_item;
}

TEST_F(CloudBinlogTransverterTest, CodeTest) {
  std::string binlog_item_s =
      PikaCloudBinlogTransverter::BinlogEncode(1, 1, 1, 1, 4294967294, 18446744073709551615, "test");
  cloud::BinlogCloudItem* binlog_item = new cloud::BinlogCloudItem();
  PikaCloudBinlogTransverter::BinlogDecode(binlog_item_s, binlog_item);
  ASSERT_EQ(1, binlog_item->db_id());
  ASSERT_EQ(1, binlog_item->rocksdb_id());
  ASSERT_EQ(1, binlog_item->exec_time());
  ASSERT_EQ(1, binlog_item->term_id());
  ASSERT_EQ(4294967294, binlog_item->file_num());          // 4294967294 = 2^32 - 1
  ASSERT_EQ(18446744073709551615, binlog_item->offset());  // 18446744073709551615 = 2^64 -1
  ASSERT_STREQ("test", binlog_item->content().c_str());
  delete binlog_item;
}

TEST_F(CloudBinlogTransverterTest, WithoutContentDecodeTest) {
  std::string binlog_item_s =
      PikaCloudBinlogTransverter::BinlogEncode(1, 1, 1, 1, 4294967294, 18446744073709551615, "test");
  cloud::BinlogCloudItem* binlog_item = new cloud::BinlogCloudItem();
  PikaCloudBinlogTransverter::BinlogItemWithoutContentDecode(binlog_item_s, binlog_item);
  ASSERT_EQ(1, binlog_item->db_id());
  ASSERT_EQ(1, binlog_item->rocksdb_id());
  ASSERT_EQ(1, binlog_item->exec_time());
  ASSERT_EQ(1, binlog_item->term_id());
  ASSERT_EQ(4294967294, binlog_item->file_num());          // 4294967294 = 2^32 - 1
  ASSERT_EQ(18446744073709551615, binlog_item->offset());  // 18446744073709551615 = 2^64 -1
  ASSERT_STREQ("", binlog_item->content().c_str());
  delete binlog_item;
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}