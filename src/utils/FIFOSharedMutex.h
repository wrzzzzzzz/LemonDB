#ifndef PROJECT_FIFOSHAREDMUTEX_H_
#define PROJECT_FIFOSHAREDMUTEX_H_
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

/*
 * FIFOSharedMutex is a mutex that allows multiple readers and one writer.
 * It is a FIFO queue, so the first thread to request a lock is the first
 * thread to get it.
 * The FIFO part comes from
 * https://codeistry.wordpress.com/2018/03/21/fifo-mutex/
 */

class FIFOSharedMutex {
private:
  std::mutex mutex;
  std::condition_variable cv;
  int readers;
  bool writer;
  std::queue<std::thread::id> queue;

public:
  FIFOSharedMutex() : readers(0), writer(false) {}

  void lock_shared(bool unlock_global = false);
  void unlock_shared();
  void lock(bool unlock_global = false);
  void unlock();
  // bool try_lock_shared(bool unlock_global = false);
  bool try_lock(bool unlock_global = false);
};

class GlobalEntranceMutex : public FIFOSharedMutex {
  static std::unique_ptr<GlobalEntranceMutex> instance;
  GlobalEntranceMutex() = default;

public:
  static GlobalEntranceMutex &getInstance();

  GlobalEntranceMutex &operator=(const GlobalEntranceMutex &) = delete;

  GlobalEntranceMutex &operator=(GlobalEntranceMutex &&) = delete;

  GlobalEntranceMutex(const GlobalEntranceMutex &) = delete;

  GlobalEntranceMutex(GlobalEntranceMutex &&) = delete;
};

class unique_FIFOlock {
  FIFOSharedMutex *m;

public:
  explicit unique_FIFOlock(FIFOSharedMutex &mut) : m(&mut) { m->lock(true); }
  ~unique_FIFOlock() { m->unlock(); }
};

class shared_FIFOlock {
  FIFOSharedMutex *m;

public:
  explicit shared_FIFOlock(FIFOSharedMutex &mut) : m(&mut) {
    m->lock_shared(true);
  }
  ~shared_FIFOlock() { m->unlock_shared(); }
};

#endif // PROJECT_FIFOSHAREDMUTEX_H_
