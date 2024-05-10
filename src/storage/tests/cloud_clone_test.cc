#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/Object.h>
#include <gtest/gtest.h>
#include <atomic>
#include <iostream>
#include <queue>
#include <thread>
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
std::string kClonePath = "clone";
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

std::string kRegion = "us-east-1";

TEST_F(CloudTest, test_360_s3) {
  // cloud environment config options here
  CloudFileSystemOptions cloud_fs_options;

  cloud_fs_options.endpoint_override = "beijing2.xstore.qihoo.net";
  cloud_fs_options.credentials.InitializeSimple("YHDIJ1LCITN7YHLETHLW", "fR5b2hEOzeogmiR01FzvYpb9BNt8eSrt0crHy510");
  if (!cloud_fs_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return;
  }

  std::string bucketName = "xx";
  cloud_fs_options.src_bucket.SetBucketName("pulsar-s3-test-beijing2");
  cloud_fs_options.dest_bucket.SetBucketName("pulsar-s3-test-beijing2");
  // Create a new AWS cloud env Status
  CloudFileSystem* cfs;
  Status s = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), "pulsar-s3-test-beijing2", kDBPath, kRegion, "pulsar-s3-test-beijing2",
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
    fprintf(stderr, "--------------xxx Unable to open db at path %s in bucket %s. %s\n",
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

  delete db;

  fprintf(stdout, "Successfully used db at %s and clone at %s in bucket %s.\n",
          kDBPath.c_str(), kClonePath.c_str(), bucketName.c_str());
}
/*
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
  kBucketSuffix2_src.append(user);
  kBucketSuffix2_dest.append(user);

  const std::string bucketPrefix = "rockset.";
  // create a bucket name for debugging purposes
  const std::string bucketName_src = bucketPrefix + kBucketSuffix2_src;
  const std::string bucketName_dest = bucketPrefix + kBucketSuffix2_src;
  //const std::string bucketName_dest = bucketPrefix + kBucketSuffix2_dest;

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
    fprintf(stderr, "iiiiii-----------------Unable to open clone at path %s in bucket %s. %s\n",
            kClonePath.c_str(), kBucketSuffix2_src.c_str(), st.ToString().c_str());
    return st;
  }
  //std::unique_ptr<DBCloud> cloud_db2;
  cloud_db->reset(db);
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
  /*char* user = getenv("USER");
  kBucketSuffix.append(user);

  const std::string bucketPrefix = "rockset.";
  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;

  // Needed if using bucket prefix other than the default "rockset."
  cloud_fs_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  cloud_fs_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);*//*
  std::string bucketName = "xx";
  cloud_fs_options.src_bucket.SetBucketName("database", "pika.");
  cloud_fs_options.dest_bucket.SetBucketName("database", "pika.");
  // Create a new AWS cloud env Status
  CloudFileSystem* cfs;
  Status s = CloudFileSystem::NewAwsFileSystem(
      FileSystem::Default(), "database", kDBPath, kRegion, "database",
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
    fprintf(stderr, "--------------xxx Unable to open db at path %s in bucket %s. %s\n",
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
    fprintf(stderr, "-------yy----Unable to clone db at path %s in bucket %s. %s\n",
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

  clone_db->Savepoint();
 //clone_db->Flush(FlushOptions());
 clone_db.release();

 delete db;

  fprintf(stdout, "Successfully used db at %s and clone at %s in bucket %s.\n",
          kDBPath.c_str(), kClonePath.c_str(), bucketName.c_str());
}*/
/*
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
TEST_F(CloudTest, del_bucket_s3) {
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  Aws::Client::ClientConfiguration cfg;
  cfg.endpointOverride = "10.224.129.40:9000";
  cfg.scheme = Aws::Http::Scheme::HTTP;
  cfg.verifySSL = false;

  Aws::Auth::AWSCredentials cred("minioadmin", "minioadmin");  // ak,sk
  Aws::S3::S3Client s3_client(cred, cfg,
                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                              false, Aws::S3::US_EAST_1_REGIONAL_ENDPOINT_OPTION::NOT_SET);

  auto response = s3_client.ListBuckets();
  if (response.IsSuccess()) {
    auto buckets = response.GetResult().GetBuckets();
    for (auto iter = buckets.begin(); iter != buckets.end(); ++iter) {
      std::cout << iter->GetName() << "\t" << iter->GetCreationDate().ToLocalTimeString(Aws::Utils::DateFormat::ISO_8601) << std::endl;
    }
  } else {
    std::cout << "Error while ListBuckets " << response.GetError().GetExceptionName()
         << " " << response.GetError().GetMessage() << std::endl;
  }



  // Aws::S3::S3Client s3_client;
  Aws::S3::Model::DeleteBucketRequest request;
  request.SetBucket("rockset.cloud2.clone.example.dst.charlieqiao");
  //s3_client.DeleteBucketAsync(request);

  Aws::S3::Model::ListObjectsRequest requ;
  requ.SetBucket("rockset.cloud2.clone.example.dst.charlieqiao");

  bool truncated = false;
  do
  {
    auto outcome = s3_client.ListObjects(requ);
    if (outcome.IsSuccess())
    {
      std::cout << "list....obinect" << std::endl;
      for (const auto& object : outcome.GetResult().GetContents())
      {
        Aws::S3::Model::DeleteObjectRequest request;
        std::cout << "Folder: " << object.GetKey() << std::endl;
        request.SetBucket("rockset.cloud2.clone.example.dst.charlieqiao");
        request.SetKey(object.GetKey());
        auto outcome = s3_client.DeleteObject(request);
        if (outcome.IsSuccess()) {
          std::cout << "File deleted successfully" << std::endl;
        } else {
          std::cout << "Failed to delete file:" << outcome.GetError().GetMessage() << std::endl;
        }
      }

      // 检查是否有下一页
      truncated = outcome.GetResult().GetIsTruncated();
      if (truncated)
      {
        requ.SetMarker(outcome.GetResult().GetNextMarker());
      }
    }
    else
    {
      std::cout << "ListObjects error: " << outcome.GetError().GetMessage() << std::endl;
      break;
    }
  } while (truncated);

  auto outcome = s3_client.DeleteBucket(request);
  if (!outcome.IsSuccess()) {
    std::cout << "DeleteBucket error: " << outcome.GetError().GetMessage() << std::endl;
  }

  Aws::ShutdownAPI(options);
}*/

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
