#include "ThreadPool.h"

#include <iostream>
#include <utility>

#include "../query/management/QuitQuery.h"

std::unique_ptr<ThreadPool> ThreadPool::instance = nullptr;

ThreadPool &ThreadPool::getInstance() {
  if (ThreadPool::instance == nullptr) {
    instance = std::unique_ptr<ThreadPool>(new ThreadPool);
  }
  return *instance;
}

void ThreadPool::ThreadWorker() {
  OutputBuffer &obuffer = OutputBuffer::getInstance();
  std::thread::id const id = std::this_thread::get_id();
  while (true) {
    if (thread_ids.find(id) == thread_ids.end())
      return;
    std::pair<size_t, Query::Ptr> query;
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      cv.wait(lock, [&] { return stop || !tasks.empty(); });
      if (stop && tasks.empty())
        return;
      if (!tasks.empty()) {
        query = std::move(tasks.front());
        tasks.pop();
        // std::cerr << query.first << ": Ranking for task " <<
        // query.second->toString() << " by thread " <<
        // std::this_thread::get_id() << std::endl;
        global_mutex.lock();
        // std::cerr << query.first << ": Start executing task " <<
        // query.second->toString() << " by thread " <<
        // std::this_thread::get_id() << std::endl;
      }
    }
    if (!resized) {
      if (query.second->toString()[8] == 'L') {
        table_size++;
      } else {
        resized = true;
        resize(table_size);
      }
    }
    QueryResult::Ptr result = query.second->execute();
    if (query.second->toString() == "QUERY = Quit") {
      obuffer.output();
      return;
    }
    OutputBubble ob(query.first);
    if (result->success()) {
      if (result->display()) {
        ob.get_out() << *result;
        // ob.error_msg << query.first << ": ";
        result->output_err(ob.get_err());
      } else {
#ifndef NDEBUG
        // ob.error_msg  << query.first << ": ";
        result->output_err(ob.get_err());
#endif
      }
    } else {
      ob.get_err() << "QUERY FAILED:\n\t" << *result;
    }
    obuffer.push(std::move(ob));
    obuffer.output();
  }
}

void ThreadPool::stopAll() {
  {
    std::unique_lock<std::mutex> const lock(queue_mutex);
    stop = true;
  }
  cv.notify_all();
}

// void ThreadPool::submit(const std::function<void()>& task) {
//     std::cout << "Pushing tasks" << std::endl;
//     std::unique_lock<std::mutex> const lock(queue_mutex);
//     tasks.push(task);
//     fflush(stdout);
//     cv.notify_one();
// }
void ThreadPool::submit(size_t count, Query::Ptr &&query) {
  std::unique_lock<std::mutex> const lock(queue_mutex);
  tasks.push(std::make_pair(count, std::move(query)));
  fflush(stdout);
  cv.notify_one();
}

void ThreadPool::waitAll() {
  for (auto &thread : threads) {
    // std::cout << "Joining thread " << thread.get_id() << std::endl;
    if (thread.joinable() && thread.get_id() != std::this_thread::get_id())
      thread.join();
  }
}

void ThreadPool::resize(size_t numThreads) {
  if (numThreads > max_threads)
    numThreads = max_threads;
  remain_threads = max_threads - numThreads;
  // std::cerr << "Resize thread pool to " << numThreads << " threads" <<
  // std::endl;
  if (numThreads > size) {
    for (size_t i = size; i < numThreads; ++i) {
      threads.push_back(std::thread(&ThreadPool::ThreadWorker, this));
      thread_ids[threads[i].get_id()] = i;
    }
  } else if (numThreads < size) {
    for (size_t i = numThreads; i < size; ++i) {
      thread_ids.erase(threads[i].get_id());
      threads[i].detach();
    }
    threads.resize(numThreads);
    size = numThreads;
  }
}

size_t ThreadPool::reserve() {
  if (remain_threads <= 0)
    return 1;
  if (reserved.try_lock()) {
    return remain_threads;
  } else {
    return 1;
  }
}

void ThreadPool::release() { reserved.unlock(); }
