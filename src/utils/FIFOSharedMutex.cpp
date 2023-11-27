#include <memory>

#include "FIFOSharedMutex.h"

void FIFOSharedMutex::lock_shared(bool unlock_global) {
  std::unique_lock<std::mutex> lock_mtx(mutex);
  queue.push(std::this_thread::get_id());
  if (unlock_global) {
    auto &gm = GlobalEntranceMutex::getInstance();
    gm.unlock();
  }
  cv.wait(lock_mtx, [this] {
    return !writer && queue.front() == std::this_thread::get_id();
  });
  ++readers;
  queue.pop();
}

void FIFOSharedMutex::unlock_shared() {
  std::unique_lock<std::mutex> const lock_mtx(mutex);
  --readers;
  if (readers == 0) {
    cv.notify_all();
  }
}

void FIFOSharedMutex::lock(bool unlock_global) {
  std::unique_lock<std::mutex> lock_mtx(mutex);
  queue.push(std::this_thread::get_id());
  if (unlock_global) {
    auto &gm = GlobalEntranceMutex::getInstance();
    gm.unlock();
  }
  cv.wait(lock_mtx, [this] {
    return !writer && readers == 0 &&
           queue.front() == std::this_thread::get_id();
  });
  writer = true;
  queue.pop();
}

void FIFOSharedMutex::unlock() {
  std::unique_lock<std::mutex> const lock_mtx(mutex);
  writer = false;
  cv.notify_all();
}

// bool FIFOSharedMutex::try_lock_shared(bool unlock_global) {
//   std::unique_lock<std::mutex> const lock_mtx(mutex);
//   if (writer) {
//     return false;
//   }
//   if (unlock_global) {
//     auto &gm = GlobalEntranceMutex::getInstance();
//     gm.unlock();
//   }
//   ++readers;
//   return true;
// }

bool FIFOSharedMutex::try_lock(bool unlock_global) {
  std::unique_lock<std::mutex> const lock_mtx(mutex);
  if (writer || readers != 0) {
    return false;
  }
  if (unlock_global) {
    auto &gm = GlobalEntranceMutex::getInstance();
    gm.unlock();
  }
  writer = true;
  return true;
}

std::unique_ptr<GlobalEntranceMutex> GlobalEntranceMutex::instance = nullptr;

GlobalEntranceMutex &GlobalEntranceMutex::getInstance() {
  static std::mutex mutex;
  std::unique_lock<std::mutex> const lock(mutex);
  if (GlobalEntranceMutex::instance == nullptr) {
    // std::cerr << "Info: create global mutex by thread " <<
    // std::this_thread::get_id() << std::endl;
    instance = std::unique_ptr<GlobalEntranceMutex>(new GlobalEntranceMutex);
  }
  return *instance;
}
