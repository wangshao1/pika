//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <dirent.h>
#include <utility>

#include "include/pika_conf.h"
#include "storage/backupable.h"
#include "storage/storage.h"

extern const std::string kRegion;

namespace storage {

BackupEngine::~BackupEngine() {
  // Wait all children threads
  StopBackup();
  WaitBackupPthread();
}

Status BackupEngine::NewCheckpoint(rocksdb::DB* rocksdb_db, int index) {
  rocksdb::DBCheckpoint* checkpoint;
  Status s = rocksdb::DBCheckpoint::Create(rocksdb_db, &checkpoint);
  if (!s.ok()) {
    return s;
  }
  engines_.insert(std::make_pair(index, std::unique_ptr<rocksdb::DBCheckpoint>(checkpoint)));
  return s;
}

Status BackupEngine::Open(storage::Storage* storage, std::shared_ptr<BackupEngine>& backup_engine_ret, int inst_count) {
  // BackupEngine() is private, can't use make_shared
  backup_engine_ret = std::shared_ptr<BackupEngine>(new BackupEngine());
  if (!backup_engine_ret) {
    return Status::Corruption("New BackupEngine failed!");
  }

  // Create BackupEngine for each rocksdb instance
  rocksdb::Status s;
  rocksdb::DB* rocksdb_db;
  for (int index = 0; index < inst_count; index++) {
    if (!(rocksdb_db = storage->GetDBByIndex(index))) {
      s = Status::Corruption("Invalid db index");
    }

    if (s.ok()) {
      s = backup_engine_ret->NewCheckpoint(rocksdb_db, index);
    }

    if (!s.ok()) {
      backup_engine_ret = nullptr;
      break;
    }
  }
  return s;
}

Status BackupEngine::SetBackupContent() {
  Status s;
  for (const auto& engine : engines_) {
    // Get backup content
    BackupContent bcontent;
    s = engine.second->GetCheckpointFiles(bcontent.live_files, bcontent.live_wal_files, bcontent.manifest_file_size,
                                          bcontent.sequence_number);
    if (!s.ok()) {
      return s;
    }
    backup_content_[engine.first] = std::move(bcontent);
  }
  return s;
}

Status BackupEngine::CreateNewBackupSpecify(const std::string& backup_dir, int index) {
  auto it_engine = engines_.find(index);
  auto it_content = backup_content_.find(index);
  std::string dir = GetSaveDirByIndex(backup_dir, index);
  delete_dir(dir.c_str());

  if (it_content != backup_content_.end() && it_engine != engines_.end()) {
    Status s = it_engine->second->CreateCheckpointWithFiles(
        dir, it_content->second.live_files, it_content->second.live_wal_files, it_content->second.manifest_file_size,
        it_content->second.sequence_number);
    if (!s.ok()) {
      //    type.c_str(), s.ToString().c_str());
      return s;
    }

  } else {
    return Status::Corruption("Invalid db index");
  }
  return Status::OK();
}

void* ThreadFuncSaveSpecify(void* arg) {
  auto arg_ptr = static_cast<BackupSaveArgs*>(arg);
  auto p = static_cast<BackupEngine*>(arg_ptr->p_engine);
  arg_ptr->res = p->CreateNewBackupSpecify(arg_ptr->backup_dir, arg_ptr->index_);
  pthread_exit(&(arg_ptr->res));
}

Status BackupEngine::WaitBackupPthread() {
  int ret;
  Status s = Status::OK();
  for (auto& pthread : backup_pthread_ts_) {
    void* res;
    if (pthread_join(pthread.second, &res) != 0) {
    }
    Status cur_s = *(static_cast<Status*>(res));
    if (!cur_s.ok()) {
      StopBackup();  // stop others when someone failed
      s = cur_s;
    }
  }
  backup_pthread_ts_.clear();
  return s;
}

Status BackupEngine::CreateNewBackup(const std::string& dir) {
  Status s = Status::OK();
  // ensure cleaning up the pointers after the function has finished.
  std::vector<std::unique_ptr<BackupSaveArgs>> args;
  args.reserve(engines_.size());
  for (const auto& engine : engines_) {
    pthread_t tid;
    auto arg = std::make_unique<BackupSaveArgs>(reinterpret_cast<void*>(this), dir, engine.first);
    args.push_back(std::move(arg));
    if (pthread_create(&tid, nullptr, &ThreadFuncSaveSpecify, args.back().get()) != 0) {
      s = Status::Corruption("pthread_create failed.");
      break;
    }
    if (!(backup_pthread_ts_.insert(std::make_pair(engine.first, tid)).second)) {
      backup_pthread_ts_[engine.first] = tid;
    }
  }

  // Wait threads stop
  if (!s.ok()) {
    StopBackup();
  }
  s = WaitBackupPthread();

  return s;
}

std::string GenBackUpDirectory(std::string& db_path) {
  // dbpath :1."db/"、2."db"、3."bak/db"
  size_t lastSepPos = db_path.rfind('/');
  if (lastSepPos != std::string::npos) {
    if (db_path.back() == '/') {
      db_path.replace(lastSepPos, std::string::npos, "_bak/");
      return db_path;
    } else {
      return db_path + "_bak/";
    }
  } else {
    return db_path.append("_bak/");
  }
}

std::string DBPath(const std::string& path, const std::string& db_name) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/", db_name.data());
  return path + buf;
}

Status BackupEngine::CreateNewCloudBackup(rocksdb::CloudFileSystemOptions& cloud_fs_options,
                                          PikaConf* pika_conf) {
  Status s = Status::OK();

  std::string src_bucket = cloud_fs_options.src_bucket.GetBucketName();
  cloud_fs_options.src_bucket.SetBucketName(src_bucket + ".backup");
  cloud_fs_options.dest_bucket.SetBucketName(src_bucket + ".backup");
  std::string db_path_tmp = pika_conf->db_path();
  std::string clone_path = GenBackUpDirectory(db_path_tmp);

  for (auto& db_pika : pika_conf->db_structs()) {
    std::string db_path = DBPath(pika_conf->db_path(), db_pika.db_name);
    std::string clone_db_path = DBPath(clone_path, db_pika.db_name);
    for (int i = 0; i < db_pika.db_instance_num; i++) {
      rocksdb::CloudFileSystem* cfs;
      s = rocksdb::CloudFileSystem::NewAwsFileSystem(
          rocksdb::FileSystem::Default(), src_bucket,
          db_path + std::to_string(i), kRegion,
          src_bucket + ".backup",clone_db_path + std::to_string(i),
          kRegion, cloud_fs_options, nullptr, &cfs);

      if (!s.ok()) {
        return s;
      }
      std::shared_ptr<rocksdb::FileSystem> fs(cfs);
      auto cloud_env = NewCompositeEnv(fs);
      Options options;
      options.env = cloud_env.get();
      // open clone
      rocksdb::DBCloud* db;
      s = rocksdb::DBCloud::Open(options, clone_db_path + std::to_string(i),
                                 "", 0, &db);
      if (!s.ok()) {
        return s;
      }
      db->Savepoint();
    }
  }
  return s;
}


void BackupEngine::StopBackup() {
  // DEPRECATED
}

}  // namespace storage

