#include "OutputBuffer.h"

std::unique_ptr<OutputBuffer> OutputBuffer::instance = nullptr;

OutputBuffer &OutputBuffer::getInstance() {
  static std::mutex instance_lock;
  std::unique_lock<std::mutex> const lock(instance_lock);
  if (OutputBuffer::instance == nullptr) {
    instance = std::unique_ptr<OutputBuffer>(new OutputBuffer);
  }
  return *instance;
}
