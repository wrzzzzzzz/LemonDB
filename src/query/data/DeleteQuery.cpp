#include "DeleteQuery.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "../../db/Database.h"
#include "../../utils/ThreadPool.h"

// constexpr const char *DeleteQuery::qname;

void DeleteQuery::addelement(Table *table, int left, int right) {
  auto it = table->origin_begin() + left;
  if (it.deleted())
    ++it;
  if (right == -1) {
    for (; it < table->end(); ++it) {
      if (this->evalCondition(*it)) {
        std::lock_guard<std::mutex> const lock(mtx);
        keys.push_back(it->key());
        affect++;
      }
    }
  } else {
    for (; it < table->origin_begin() + right; ++it) {
      if (this->evalCondition(*it)) {
        std::lock_guard<std::mutex> const lock(mtx);
        keys.push_back(it->key());
        affect++;
      }
    }
  }
}

QueryResult::Ptr DeleteQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (!this->get_operands().empty())
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Invalid number of operands (? operands)."_f % get_operands().size());
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    unique_FIFOlock const lock(table.get_table_mutex());
    if (this->get_condition().empty()) {
      affect = table.clear();
      return std::make_unique<RecordCountResult>(affect);
    }
    auto result = initCondition(table);
    if (result.second) {
      // for (auto it = table.begin(); it != table.end(); ++it) {
      //     if (this->evalCondition(*it)) {
      //       keys.push_back(it->key());
      //       affect ++;
      //     }
      // }
      int size = static_cast<int>(table.size());
      auto &tp = ThreadPool::getInstance();
      int const NUMOFTHREADS = static_cast<int>(tp.reserve());
      if (NUMOFTHREADS == 1) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) {
            keys.push_back(it->key());
            affect++;
          }
        }
      } else {
        size = size / NUMOFTHREADS;
        std::vector<std::thread> threads;
        for (int i = 0; i < NUMOFTHREADS; ++i) {
          if (i == NUMOFTHREADS - 1) {
            threads.push_back(std::thread(std::bind(
                &DeleteQuery::addelement, this, &table, i * size, -1)));
          } else {
            assert(i * size <= static_cast<int>(table.size()));
            threads.push_back(
                std::thread(std::bind(&DeleteQuery::addelement, this, &table,
                                      i * size, (i + 1) * size)));
          }
        }
        for (std::thread &thread : threads) {
          thread.join();
        }
        tp.release();
      }
    }
    table.erase(keys);
    // cout << "Affected " << affect << " rows" << endl;
    // return make_unique<SuccessMsgResult>(qname, this->get_target_table());
    return std::make_unique<RecordCountResult>(affect);
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

std::string DeleteQuery::toString() {
  return "QUERY = DELETE " + this->get_target_table() + "\"";
}
