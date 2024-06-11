// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "include/pika_cloud_binlog.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/time.h>

#include <utility>

#include "pstd/include/pstd_defer.h"
#include "pstd_status.h"
#include "include/pika_cloud_binlog_transverter.h"


using pstd::Status;

std::string NewCloudFileName(const std::string& name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return {buf};
}

/*
 * CloudVersion
 */
CloudVersion::CloudVersion(const std::shared_ptr<pstd::RWFile>& save) : save_(save) { assert(save_ != nullptr); }

CloudVersion::~CloudVersion() { StableSave(); }

Status CloudVersion::StableSave() {
  char* p = save_->GetData();
  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &term_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &keep_filenum_, sizeof(uint32_t));
  return Status::OK();
}

Status CloudVersion::Init() {
  Status s;
  if (save_->GetData()) {
    memcpy(reinterpret_cast<char*>(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&pro_offset_), save_->GetData() + 4, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&term_), save_->GetData() + 12, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&keep_filenum_), save_->GetData() + 16, sizeof(uint32_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

/*
 * Binlog
 */

CloudBinlog::CloudBinlog(std::string binlog_path, const int file_size)
    : Binlog("", 0),
      opened_(false),
      binlog_path_(std::move(binlog_path)),
      file_size_(file_size),
      binlog_io_error_(false) {
  // To intergrate with old version, we don't set mmap file size to 100M;
  // pstd::SetMmapBoundSize(file_size);
  // pstd::kMmapBoundSize = 1024 * 1024 * 100;
  // bin log not init
  if (binlog_path_ == "" || file_size_ == 0) return;

  Status s;
  pstd::CreateDir(binlog_path_);

  filename_ = binlog_path_ + kBinlogPrefix;
  const std::string manifest = binlog_path_ + kManifest;
  std::string profile;

  if (!pstd::FileExists(manifest)) {
    LOG(INFO) << "Cloud Binlog: Manifest file not exist, we create a new one.";

    profile = NewFileName(filename_, pro_num_);
    s = pstd::NewWritableFile(profile, queue_);
    if (!s.ok()) {
      LOG(FATAL) << "Cloud Binlog: new " << filename_ << " " << s.ToString();
    }
    std::unique_ptr<pstd::RWFile> tmp_file;
    s = pstd::NewRWFile(manifest, tmp_file);
    versionfile_.reset(tmp_file.release());
    if (!s.ok()) {
      LOG(FATAL) << "Cloud Binlog: new versionfile error " << s.ToString();
    }

    version_ = std::make_unique<CloudVersion>(versionfile_);
    version_->StableSave();
  } else {
    LOG(INFO) << "Cloud Binlog: Find the exist file.";
    std::unique_ptr<pstd::RWFile> tmp_file;
    s = pstd::NewRWFile(manifest, tmp_file);
    versionfile_.reset(tmp_file.release());
    if (s.ok()) {
      version_ = std::make_unique<CloudVersion>(versionfile_);
      version_->Init();
      pro_num_ = version_->pro_num_;

      // Debug
      // version_->debug();
    } else {
      LOG(FATAL) << "Cloud Binlog: open versionfile error";
    }

    profile = NewFileName(filename_, pro_num_);
    DLOG(INFO) << "Cloud Binlog: open profile " << profile;
    s = pstd::AppendWritableFile(profile, queue_, version_->pro_offset_);
    if (!s.ok()) {
      LOG(FATAL) << "Cloud Binlog: Open file " << profile << " error " << s.ToString();
    }

    uint64_t filesize = queue_->Filesize();
    DLOG(INFO) << "Cloud Binlog: filesize is " << filesize;
  }

  InitLogFile();
}

CloudBinlog::~CloudBinlog() {
  std::lock_guard l(mutex_);
  Close();
}

void CloudBinlog::Close() {
  if (!opened_.load()) {
    return;
  }
  opened_.store(false);
}

void CloudBinlog::InitLogFile() {
  assert(queue_ != nullptr);
  uint64_t filesize = queue_->Filesize();
  block_offset_ = static_cast<int32_t>(filesize % kBlockSize);
  opened_.store(true);
}

Status CloudBinlog::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint32_t* term, uint64_t* logic_id) {
  if (!opened_.load()) {
    return Status::Busy("Cloud Binlog is not open yet");
  }

  std::shared_lock l(version_->rwlock_);

  *filenum = version_->pro_num_;
  *pro_offset = version_->pro_offset_;
  if (term) {
    *term = version_->term_;
  }

  return Status::OK();
}

Status CloudBinlog::GetOldestBinlogToKeep(uint32_t* filenum, uint32_t* term, uint64_t* logic_id) {
  if (!opened_.load()) {
    return Status::Busy("Cloud Binlog is not open yet");
  }

  std::shared_lock l(version_->rwlock_);
  *filenum = version_->keep_filenum_;
  if (term) {
    *term = version_->term_;
  }
  return Status::OK();
}

Status CloudBinlog::Put(const std::string& item) {
  if (!opened_.load()) {
    return Status::Busy("Cloud Binlog is not open yet");
  }

  Lock();
  DEFER { Unlock(); };

  Status s = Put(item.c_str(), static_cast<int>(item.size()));
  if (!s.ok()) {
    binlog_io_error_.store(true);
  }
  return s;
}
// Note: mutex lock should be held
Status CloudBinlog::Put(const std::string& item, uint32_t db_id, uint32_t rocksdb_id, uint32_t type) {
  if (!opened_.load()) {
    return Status::Busy("Cloud Binlog is not open yet");
  }
  uint32_t filenum = 0;
  uint32_t term = 0;
  uint64_t offset = 0;

  Lock();
  DEFER { Unlock(); };

  Status s = GetProducerStatus(&filenum, &offset, &term, nullptr);
  if (!s.ok()) {
    return s;
  }
  std::string data = PikaCloudBinlogTransverter::BinlogEncode(db_id, rocksdb_id, time(nullptr), term, filenum, offset, item, type);

  s = Put(data.c_str(), static_cast<int>(data.size()));
  if (!s.ok()) {
    binlog_io_error_.store(true);
  }
  // record first binlog item and manifest update binlog item
  if (type != 0 || binlog_to_keep_.find(rocksdb_id) == binlog_to_keep_.end()) {
    binlog_to_keep_[rocksdb_id] = filenum;
  }

  uint32_t keep_filenum = binlog_to_keep_.begin()->second;
  for (const auto& offset : binlog_to_keep_) {
    keep_filenum = std::min(keep_filenum, offset.second); 
  }

  version_->keep_filenum_ = keep_filenum;
  return s;
}

// Note: mutex lock should be held
Status CloudBinlog::Put(const char* item, int len) {
  Status s;
  /* Check to roll log file */
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    std::unique_ptr<pstd::WritableFile> queue;
    std::string profile = NewCloudFileName(filename_, pro_num_ + 1);
    s = pstd::NewWritableFile(profile, queue);
    if (!s.ok()) {
      LOG(ERROR) << "Cloud Binlog: new " << filename_ << " " << s.ToString();
      return s;
    }
    queue_.reset();
    queue_ = std::move(queue);
    pro_num_++;

    {
      std::lock_guard l(version_->rwlock_);
      version_->pro_offset_ = 0;
      version_->pro_num_ = pro_num_;
      version_->StableSave();
    }
    InitLogFile();
  }

  int pro_offset = 0;
  s = Produce(pstd::Slice(item, len), &pro_offset);
  if (s.ok()) {
    std::lock_guard l(version_->rwlock_);
    version_->pro_offset_ = pro_offset;
    version_->StableSave();
  }

  return s;
}

Status CloudBinlog::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n, int* temp_pro_offset) {
  Status s;
  assert(n <= 0xffffff);
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);
  char buf[kHeaderSize];

  uint64_t now = 0;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  now = tv.tv_sec;
  buf[0] = static_cast<char>(n & 0xff);
  buf[1] = static_cast<char>((n & 0xff00) >> 8);
  buf[2] = static_cast<char>(n >> 16);
  buf[3] = static_cast<char>(now & 0xff);
  buf[4] = static_cast<char>((now & 0xff00) >> 8);
  buf[5] = static_cast<char>((now & 0xff0000) >> 16);
  buf[6] = static_cast<char>((now & 0xff000000) >> 24);
  buf[7] = static_cast<char>(t);

  s = queue_->Append(pstd::Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = queue_->Append(pstd::Slice(ptr, n));
    if (s.ok()) {
      s = queue_->Flush();
    }
  }
  block_offset_ += static_cast<int32_t>(kHeaderSize + n);

  *temp_pro_offset += static_cast<int32_t>(kHeaderSize + n);
  return s;
}

Status CloudBinlog::Produce(const pstd::Slice& item, int* temp_pro_offset) {
  Status s;
  const char* ptr = item.data();
  size_t left = item.size();
  bool begin = true;

  *temp_pro_offset = static_cast<int>(version_->pro_offset_);
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;
    assert(leftover >= 0);
    if (static_cast<size_t>(leftover) < kHeaderSize) {
      if (leftover > 0) {
        s = queue_->Append(pstd::Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        if (!s.ok()) {
          return s;
        }
        *temp_pro_offset += leftover;
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;
    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, temp_pro_offset);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  return s;
}

Status CloudBinlog::AppendPadding(pstd::WritableFile* file, uint64_t* len) {
  if (*len < kHeaderSize) {
    return Status::OK();
  }

  Status s;
  char buf[kBlockSize];
  uint64_t now = 0;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  now = tv.tv_sec;

  uint64_t left = *len;
  while (left > 0 && s.ok()) {
    uint32_t size = (left >= kBlockSize) ? kBlockSize : left;
    if (size < kHeaderSize) {
      break;
    } else {
      uint32_t bsize = size - kHeaderSize;
      std::string binlog(bsize, '*');
      buf[0] = static_cast<char>(bsize & 0xff);
      buf[1] = static_cast<char>((bsize & 0xff00) >> 8);
      buf[2] = static_cast<char>(bsize >> 16);
      buf[3] = static_cast<char>(now & 0xff);
      buf[4] = static_cast<char>((now & 0xff00) >> 8);
      buf[5] = static_cast<char>((now & 0xff0000) >> 16);
      buf[6] = static_cast<char>((now & 0xff000000) >> 24);
      // kBadRecord here
      buf[7] = static_cast<char>(kBadRecord);
      s = file->Append(pstd::Slice(buf, kHeaderSize));
      if (s.ok()) {
        s = file->Append(pstd::Slice(binlog.data(), binlog.size()));
        if (s.ok()) {
          s = file->Flush();
          left -= size;
        }
      }
    }
  }
  *len -= left;
  if (left != 0) {
    LOG(WARNING) << "Cloud AppendPadding left bytes: " << left << " is less then kHeaderSize";
  }
  return s;
}

Status CloudBinlog::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset, uint32_t term, uint64_t index) {
  if (!opened_.load()) {
    return Status::Busy("Cloud Binlog is not open yet");
  }

  std::lock_guard l(mutex_);

  // offset smaller than the first header
  if (pro_offset < 4) {
    pro_offset = 0;
  }

  queue_.reset();

  std::string init_profile = NewCloudFileName(filename_, 0);
  if (pstd::FileExists(init_profile)) {
    pstd::DeleteFile(init_profile);
  }

  std::string profile = NewCloudFileName(filename_, pro_num);
  if (pstd::FileExists(profile)) {
    pstd::DeleteFile(profile);
  }

  pstd::NewWritableFile(profile, queue_);
  CloudBinlog::AppendPadding(queue_.get(), &pro_offset);

  pro_num_ = pro_num;

  {
    std::lock_guard l(version_->rwlock_);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->term_ = term;
    version_->StableSave();
  }

  InitLogFile();
  return Status::OK();
}

Status CloudBinlog::Truncate(uint32_t pro_num, uint64_t pro_offset, uint64_t index) {
  queue_.reset();
  std::string profile = NewCloudFileName(filename_, pro_num);
  const int fd = open(profile.c_str(), O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IOError("fd open failed");
  }
  if (ftruncate(fd, static_cast<int64_t>(pro_offset)) != 0) {
    return Status::IOError("ftruncate failed");
  }
  close(fd);

  pro_num_ = pro_num;
  {
    std::lock_guard l(version_->rwlock_);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->StableSave();
  }

  Status s = pstd::AppendWritableFile(profile, queue_, version_->pro_offset_);
  if (!s.ok()) {
    return s;
  }

  InitLogFile();

  return Status::OK();
}