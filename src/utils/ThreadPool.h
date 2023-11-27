#ifndef PROJECT_THREADPOOL_H_
#define PROJECT_THREADPOOL_H_

#include "FIFOSharedMutex.h"
#include "OutputBuffer.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../query/Query.h"

class ThreadPool {
  std::queue<std::pair<size_t, Query::Ptr>> tasks;
  std::unordered_map<std::thread::id, size_t> thread_ids;
  std::atomic<size_t> size, table_size, remain_threads, max_threads;
  std::atomic<bool> resized;
  std::mutex reserved;
  std::vector<std::thread> threads;
  std::mutex queue_mutex;
  std::condition_variable cv;
  static std::unique_ptr<ThreadPool> instance;
  GlobalEntranceMutex &global_mutex = GlobalEntranceMutex::getInstance();

  void ThreadWorker();
  ThreadPool() = default;
  bool stop = false;

public:
  static ThreadPool &getInstance();
  bool is_stopped() { return stop; }
  void init(size_t threadnum) {
    table_size = 0;
    resized = false;
    for (size_t i = 0; i < threadnum; i++) {
      threads.push_back(std::thread(&ThreadPool::ThreadWorker, this));
      thread_ids[threads[i].get_id()] = i;
    }
    size = threadnum;
    max_threads = threadnum;
    remain_threads = 0;
  }

  ThreadPool &operator=(const ThreadPool &) = delete;

  ThreadPool &operator=(ThreadPool &&) = delete;

  ThreadPool(const ThreadPool &) = delete;

  ThreadPool(ThreadPool &&) = delete;

  ~ThreadPool() { waitAll(); }

  void stopAll();

  // void submit(const std::function<void()>& task);
  void submit(size_t count, Query::Ptr &&query);

  void waitAll();

  void resize(size_t numThreads);

  size_t reserve();

  void release();
};

#endif // PROJECT_THREADPOOL_H_
