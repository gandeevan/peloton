#pragma once

#include "common/macros.h"
#include "type/types.h"

namespace peloton {


class OutputBuffer {
  OutputBuffer(const OutputBuffer &) = delete;
  OutputBuffer &operator=(const OutputBuffer &) = delete;
  OutputBuffer(OutputBuffer &&) = delete;
  OutputBuffer &operator=(OutputBuffer &&) = delete;

private:
  // constant
  const static size_t buffer_capacity_ = 1024 * 1024 * 32; // 32 MB

public:
  OutputBuffer() :
     size_(0){
    data_ = new char[buffer_capacity_];
    PL_MEMSET(data_, 0, buffer_capacity_);
  }
  ~OutputBuffer() {
    delete[] data_;
    data_ = nullptr;
  }

  inline void Reset() { size_ = 0; }

  inline char *GetData() { return data_; }

  inline size_t GetSize() { return size_; }

  inline bool Empty() { return size_ == 0; }

  bool WriteData(const char *data, size_t len){
      if (unlikely_branch(size_ + len > buffer_capacity_)) {
        return false;
      } else {
        PL_ASSERT(data);
        PL_ASSERT(len);
        PL_MEMCPY(data_ + size_, data, len);
        size_ += len;
        return true;
      }
  }

private:
  size_t size_;
  char* data_;
};
}
