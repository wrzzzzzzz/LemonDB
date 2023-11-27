#include "CountQuery.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../../db/Database.h"
#include "../../utils/ThreadPool.h"
#include "../QueryResult.h"

// constexpr const char *CountQuery::qname;

void CountQuery::findsize(Table *table, int left, int right) {
  size_t local_size = 0;
  auto it = table->origin_begin() + left;
  if (it.deleted())
    ++it;
  if (right == -1) {
    for (; it != table->end(); ++it) {
      if (this->evalCondition(*it)) {
        local_size++;
      }
    }
    std::lock_guard<std::mutex> const lock(mtx);
    this->sizes.push_back(local_size);
  } else {
    for (; it < table->origin_begin() + right; ++it) {
      if (this->evalCondition(*it)) {
        local_size++;
      }
    }
    std::lock_guard<std::mutex> const lock(mtx);
    this->sizes.push_back(local_size);
  }
}

// e.g. COUNT ( ) FROM Student;
//  should operate based on given columns, say, fieldnames
QueryResult::Ptr CountQuery::execute() {
  using std::literals::string_literals::operator""s;
  Database &db = Database::getInstance(); // get the database
  try {
    auto &table = db[this->get_target_table()]; // get the table to operate
    shared_FIFOlock const lock(table.get_table_mutex());
    // e.g. COUNT ( ) FROM Student;
    // ANSWER = 2
    auto result = initCondition(table);
    size_t cnt = 0;
    if (result.second) { // meet the WHERE sentence(?)
      // iterate through rows
      int size = static_cast<int>(table.size());
      auto &tp = ThreadPool::getInstance();
      int const NUMOFTHREADS = static_cast<int>(tp.reserve());
      if (NUMOFTHREADS == 1) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) { // row meets the condition in WHERE
            cnt++;
          }
        }
      } else {
        size = size / NUMOFTHREADS;
        // thread threads[NUMOFTHREADS];
        std::vector<std::thread> threads;
        for (int i = 0; i < NUMOFTHREADS; i++) {
          if (i == NUMOFTHREADS - 1) {
            threads.push_back(std::thread(
                std::bind(&CountQuery::findsize, this, &table, i * size, -1)));
          } else {
            assert(i * size <= static_cast<int>(table.size()));
            threads.push_back(
                std::thread(std::bind(&CountQuery::findsize, this, &table,
                                      i * size, (i + 1) * size)));
          }
        }

        // wait for threads to finish
        // for(int i=0;i<NUMOFTHREADS;++i){
        // 	threads[i].join();
        // }
        for (auto &thread : threads)
          thread.join();

        // sum up the counts
        for (size_t i = 0; i < this->sizes.size(); i++) {
          cnt += this->sizes[i];
        }
        tp.release();
      }
    } else {
      return std::make_unique<SuccessMsgResult>(0); // if (!result.second)
    }
    return std::make_unique<SuccessMsgResult>(cnt); // meant to be:  ANSWER = 2

  } catch (const TableNameNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            "No such table."s);
  } catch (const IllFormedQueryCondition &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            e.what());
  } catch (const std::invalid_argument &e) {
    // Cannot convert operand to string
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            "Unknown error '?'"_f % e.what());
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            "Unkonwn error '?'."_f % e.what());
  }
}

std::string CountQuery::toString() {
  return "QUERY = COUNT " + this->get_target_table() + "\"";
}
