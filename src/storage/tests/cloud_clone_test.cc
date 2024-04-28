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
#include "rocksdb/cloud/db_cloud.h"

using namespace storage;
using namespace rocksdb;


class CloudTest : public ::testing::Test {
public:
  CloudTest() = default;
  ~CloudTest() override = default;

  void SetUp() override {
    storage_options.options.create_if_missing = true;
    storage_options.options.avoid_flush_during_shutdown = true;
    auto& cloud_fs_opts = storage_options.cloud_fs_options;
    cloud_fs_opts.endpoint_override = "http://10.224.129.40:9000";
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

// This is the local directory where the db is stored. The same
// path name is used to store data inside the specified cloud
// storage bucket.
std::string kDBPath = "db";

// This is the local directory where the clone is stored. The same
// pathname is used to store data in the specified cloud bucket.
//std::string kClonePath = "db";
std::string kClonePath = "clone_db";
std::string kBucketSuffix = "cloud.clone.example.";
std::string kBucketSuffix2_src = "cloud2.clone.example.";
std::string kBucketSuffix2_dest = "cloud2.clone.example.dst.";
//
// This is the name of the cloud storage bucket where the db
// is made durable. If you are using AWS, you have to manually
// ensure that this bucket name is unique to you and does not
// conflict with any other S3 users who might have already created
// this bucket name.
// In this example, the database and its clone are both stored in
// the same bucket (obviously with different pathnames).
//

std::string kRegion = "us-west-2";

Status CloneDB(const std::string& clone_name, const std::string& src_bucket,
               const std::string& src_object_path,
               const std::string& dest_bucket,
               const std::string& dest_object_path,
               const CloudFileSystemOptions& cloud_fs_options,
               std::unique_ptr<DBCloud>* cloud_db, std::unique_ptr<Env>* cloud_env) {
  CloudFileSystemOptions cloud_fs_options2;

  cloud_fs_options2.endpoint_override = "http://10.224.129.40:9000";
  cloud_fs_options2.credentials.InitializeSimple("minioadmin", "minioadmin");
  if (!cloud_fs_options2.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return rocksdb::Status::OK();
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-namess need to be globlly unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char* user = getenv("USER");
  kBucketSuffix2_src.append(user); kBucketSuffix2_dest.append(user);

  const std::string bucketPrefix = "rockset.";
  // create a bucket name for debugging purposes
  const std::string bucketName_src = bucketPrefix + kBucketSuffix2_src;
  const std::string bucketName_dest = bucketPrefix + kBucketSuffix2_dest;

  // Needed if using bucket prefix other than the default "rockset."
  cloud_fs_options2.src_bucket.SetBucketName(kBucketSuffix2_src, bucketPrefix);
  cloud_fs_options2.dest_bucket.SetBucketName(kBucketSuffix2_dest, bucketPrefix);

  CloudFileSystem* cfs;
  Status st = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), src_bucket, src_object_path, kRegion, kBucketSuffix2_src,
      dest_object_path, kRegion, cloud_fs_options2, nullptr, &cfs);

  if (!st.ok()) {
    fprintf(stderr,
            "Unable to create an AWS environment with "
            "bucket %s",
            src_bucket.c_str());
    return st;
  }
  std::shared_ptr<FileSystem> fs(cfs);
  *cloud_env = NewCompositeEnv(fs);

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env->get();

  // No persistent cache
  std::string persistent_cache = "";
  // open clone
  DBCloud* db;
  st = DBCloud::Open(options, kClonePath, persistent_cache, 0, &db);
  if (!st.ok()) {
    fprintf(stderr, "Unable to open clone at path %s in bucket %s. %s\n",
            kClonePath.c_str(), kBucketSuffix2_src.c_str(), st.ToString().c_str());
    return st;
  }
  //std::unique_ptr<DBCloud> cloud_db2;
  std::cout << "bx..." << std::endl;
  cloud_db->reset(db);
  std::cout << "by..." << std::endl;

  CloudFileSystem* cfs_bak;
  st = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), kBucketSuffix2_src, dest_object_path, kRegion, kBucketSuffix2_dest,
      dest_object_path, kRegion, cloud_fs_options2, nullptr, &cfs_bak);

  if (!st.ok()) {
    fprintf(stderr,
            "Unable to create an AWS environment with "
            "bucket %s",
            src_bucket.c_str());
    return st;
  }
  std::shared_ptr<FileSystem> fs_bak(cfs_bak);
  auto cloud_env_bak = NewCompositeEnv(fs_bak);
  // Create options and use the AWS env that we created earlier
  Options options2;
  options2.env = cloud_env_bak.get();

  // No persistent cache
  std::string persistent_cache_bak = "";
  // open clone
  DBCloud* db_bak;
  st = DBCloud::Open(options2, kClonePath, persistent_cache_bak, 0, &db_bak);
  if (!st.ok()) {
    fprintf(stderr, "Unable to open clone at path %s in bucket %s. %s\n",
            kClonePath.c_str(), kBucketSuffix2_src.c_str(), st.ToString().c_str());
    return st;
  }

  cloud_db->get()->Savepoint();
  //db_bak->Savepoint();
  return Status::OK();
}

TEST_F(CloudTest, clone_s3) {
  // cloud environment config options here
  CloudFileSystemOptions cloud_fs_options;

  cloud_fs_options.endpoint_override = "http://10.224.129.40:9000";
  cloud_fs_options.credentials.InitializeSimple("minioadmin", "minioadmin");
  if (!cloud_fs_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return;
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-namess need to be globlly unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char* user = getenv("USER");
  kBucketSuffix.append(user);

  const std::string bucketPrefix = "rockset.";
  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;

  // Needed if using bucket prefix other than the default "rockset."
  cloud_fs_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  cloud_fs_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);

  // Create a new AWS cloud env Status
  CloudFileSystem* cfs;
  Status s = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), kBucketSuffix, kDBPath, kRegion, kBucketSuffix,
      kDBPath, kRegion, cloud_fs_options, nullptr, &cfs);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env in bucket %s. %s\n",
            bucketName.c_str(), s.ToString().c_str());
    return;
  }


  // Store a reference to a cloud env. A new cloud env object should be
  // associated with every new cloud-db.
  auto cloud_env = NewCompositeEnv(std::shared_ptr<FileSystem>(cfs));

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env.get();
  options.create_if_missing = true;

  // No persistent cache
  std::string persistent_cache = "";

  // Create and Open DB
  DBCloud* db;
  s = DBCloud::Open(options, kDBPath, persistent_cache, 0, &db);
  if (!s.ok()) {
    fprintf(stderr, "Unable to open db at path %s in bucket %s. %s\n",
            kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
    return;
  }

  // Put key-value into main db
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;

  // get value from main db
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // Flush all data from main db to sst files.
  db->Flush(FlushOptions());

  // Create a clone of the db and and verify that all's well.
  // In real applications, a Clone would typically be created
  // by a separate process.
  //std::unique_ptr<DB> clone_db;
  std::unique_ptr<Env> clone_env;
  std::unique_ptr<DBCloud> clone_db;
  s = CloneDB("clone1", kBucketSuffix, kDBPath, kBucketSuffix, kClonePath,
              cloud_fs_options, &clone_db, &clone_env);
  if (!s.ok()) {
    fprintf(stderr, "Unable to clone db at path %s in bucket %s. %s\n",
            kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
    return;
  }

  // insert a key-value in the clone.
  s = clone_db->Put(WriteOptions(), "name", "dhruba");
  assert(s.ok());

  // assert that values from the main db appears in the clone
  s = clone_db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

 //clone_db->Flush(FlushOptions());
 clone_db.release();

 delete db;

  fprintf(stdout, "Successfully used db at %s and clone at %s in bucket %s.\n",
          kDBPath.c_str(), kClonePath.c_str(), bucketName.c_str());
}


TEST_F(CloudTest, get_clone_s3) {
  // cloud environment config options here
  CloudFileSystemOptions cloud_fs_options;

  cloud_fs_options.endpoint_override = "http://10.224.129.40:9000";
  cloud_fs_options.credentials.InitializeSimple("minioadmin", "minioadmin");
  if (!cloud_fs_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return;
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-namess need to be globlly unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char* user = getenv("USER");
  kBucketSuffix.append(user);

  const std::string bucketPrefix = "rockset.";
  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;

  // Needed if using bucket prefix other than the default "rockset."
  cloud_fs_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  cloud_fs_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);

  // Create a new AWS cloud env Status
  CloudFileSystem* cfs;
  Status s = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), kBucketSuffix, kDBPath, kRegion, kBucketSuffix,
      kDBPath, kRegion, cloud_fs_options, nullptr, &cfs);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env in bucket %s. %s\n",
            bucketName.c_str(), s.ToString().c_str());
    return;
  }


  // Store a reference to a cloud env. A new cloud env object should be
  // associated with every new cloud-db.
  auto cloud_env = NewCompositeEnv(std::shared_ptr<FileSystem>(cfs));

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env.get();
  options.create_if_missing = true;

  // No persistent cache
  std::string persistent_cache = "";

  // Create and Open DB
  DBCloud* db;
  s = DBCloud::Open(options, kDBPath, persistent_cache, 0, &db);
  if (!s.ok()) {
    fprintf(stderr, "Unable to open db at path %s in bucket %s. %s\n",
            kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
    return;
  }

  // Put key-value into main db
  std::string value;
  s = db->Get(ReadOptions(), "name", &value);
  std::cout << "value1: " << value << std::endl;
  // get value from main db
  s = db->Get(ReadOptions(), "key1", &value);
  std::cout << "value2: " << value << std::endl;
  assert(s.ok());
  assert(value == "value");
  return;
}

TEST_F(CloudTest, delete_s3) {
  // cloud environment config options here
  CloudFileSystemOptions cloud_fs_options;

  cloud_fs_options.endpoint_override = "http://10.224.129.40:9000";
  cloud_fs_options.credentials.InitializeSimple("minioadmin", "minioadmin");
  if (!cloud_fs_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return;
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-namess need to be globlly unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char* user = getenv("USER");
  kBucketSuffix.append(user);

  const std::string bucketPrefix = "rockset.";
  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;

  // Needed if using bucket prefix other than the default "rockset."
  cloud_fs_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  cloud_fs_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);

  // Create a new AWS cloud env Status
  CloudFileSystem* cfs;
  Status s = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), kBucketSuffix, kDBPath, kRegion, kBucketSuffix,
      kDBPath, kRegion, cloud_fs_options, nullptr, &cfs);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env in bucket %s. %s\n",
            bucketName.c_str(), s.ToString().c_str());
    return;
  }


  // Store a reference to a cloud env. A new cloud env object should be
  // associated with every new cloud-db.
  auto cloud_env = NewCompositeEnv(std::shared_ptr<FileSystem>(cfs));

  // Create options and use the AWS env that we created earlier
  Options options;
  options.env = cloud_env.get();
  options.create_if_missing = true;

  // No persistent cache
  std::string persistent_cache = "";

  // Create and Open DB
  DBCloud* db;
  s = DBCloud::Open(options, kDBPath, persistent_cache, 0, &db);
  if (!s.ok()) {
    fprintf(stderr, "Unable to open db at path %s in bucket %s. %s\n",
            kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
    return;
  }
  //cfs->DeleteCloudFileFromDest();

}

int main(int argc, char** argv) {
  if (!pstd::FileExists("./log")) {
    pstd::CreatePath("./log");
  }
  FLAGS_log_dir = "./log";
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("cloud_clone_test");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
