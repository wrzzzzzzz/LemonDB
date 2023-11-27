#ifndef PROJECT_OUTPUTBUFFER_H
#define PROJECT_OUTPUTBUFFER_H

#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

class OutputBubble {
  size_t id;
  std::ostringstream output_msg;
  std::ostringstream error_msg;

public:
  explicit OutputBubble(size_t id) : id(id) {}
  bool operator<(const OutputBubble &other) const { return id < other.id; }
  bool operator>(const OutputBubble &other) const { return id > other.id; }
  std::ostringstream &get_out() { return output_msg; }
  const std::ostringstream &get_out() const { return output_msg; }
  std::ostringstream &get_err() { return error_msg; }
  const std::ostringstream &get_err() const { return error_msg; }
  size_t get_id() const { return id; }
};

class OutputBuffer {
  std::mutex mtx;
  std::priority_queue<OutputBubble, std::vector<OutputBubble>,
                      std::greater<OutputBubble>>
      output_queue;
  size_t id;
  static std::unique_ptr<OutputBuffer> instance;

  OutputBuffer() { OutputBuffer::id = 1; }

public:
  static OutputBuffer &getInstance();
  void push(OutputBubble &&bubble) {
    std::lock_guard<std::mutex> const lock(mtx);
    output_queue.push(std::move(bubble));
  }

  void output() {
    std::lock_guard<std::mutex> const lock(mtx);
    while (!output_queue.empty() && output_queue.top().get_id() == id) {
      auto &bubble = output_queue.top();
      std::cout << bubble.get_id() << std::endl;
      std::cout << bubble.get_out().str();
      std::cerr << bubble.get_err().str();
      fflush(stdout);
      fflush(stderr);
      OutputBuffer::output_queue.pop();
      id++;
    }
  }
};

#endif // PROJECT_OUTPUTBUFFER_H
