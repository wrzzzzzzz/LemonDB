#include "SelectQuery.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../utils/ThreadPool.h"

// constexpr const char *SelectQuery::qname;

void SelectQuery::addobj(Table *table, int left, int right = -1) {
  auto it = table->origin_begin() + left;
  if (it.deleted())
    ++it;
  if (right == -1) {
    for (; it < table->end(); ++it) {
      if (this->evalCondition(*it)) {
        std::lock_guard<std::mutex> const lock(mtx);
        this->objs.push_back(*it);
      }
    }
  } else {
    for (; it < table->origin_begin() + right; ++it) {
      if (this->evalCondition(*it)) {
        std::lock_guard<std::mutex> const lock(mtx);
        this->objs.push_back(*it);
      }
    }
  }
}

QueryResult::Ptr SelectQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (this->get_operands().empty() || this->get_operands()[0] != "KEY")
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Invalid number of operands (? operands) or without leading KEY."_f %
            get_operands().size());
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    shared_FIFOlock const lock(table.get_table_mutex());
    for (const auto &operand : this->get_operands()) {
      if (operand == "KEY") {
        continue;
      }
      this->fieldIds.emplace_back(table.getFieldIndex(operand));
    }
    auto result = initCondition(table);
    if (result.second) {
      int size = static_cast<int>(table.size());
      auto &tp = ThreadPool::getInstance();
      int const NUMOFTHREADS = static_cast<int>(tp.reserve());
      if (NUMOFTHREADS == 1) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) {
            objs.push_back(*it);
          }
        }
      } else {
        size = size / NUMOFTHREADS;
        std::vector<std::thread> threads;
        for (int i = 0; i < NUMOFTHREADS; ++i) {
          if (i == NUMOFTHREADS - 1) {
            // threads[i] = thread(addobj, table, i*size, -1);
            threads.push_back(std::thread(
                std::bind(&SelectQuery::addobj, this, &table, i * size, -1)));
          } else {
            // threads[i] = thread(addobj, table, i*size, (i+1)*size);
            assert(i * size <= static_cast<int>(table.size()));
            threads.push_back(std::thread(std::bind(
                &SelectQuery::addobj, this, &table, i * size, (i + 1) * size)));
          }
        }
        for (std::thread &t : threads) {
          t.join();
        }
        tp.release();
      }
    }
    std::stringstream ss;
    printResult(&objs, ss);
    return std::make_unique<CustomizedResult>(qname, this->get_target_table(),
                                              std::move(ss));
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

std::string SelectQuery::toString() {
  return "QUERY = SELECT " + this->get_target_table() + "\"";
}

void SelectQuery::printResult(std::vector<Table::Object> *objs,
                              std::stringstream &ss) {
  std::sort(objs->begin(), objs->end(),
            [](Table::Object obj1, Table::Object obj2) {
              return obj1.key() < obj2.key();
            });
  for (auto obj : *objs) {
    ss << "( " << obj.key() << " ";
    for (auto fieldId : this->fieldIds)
      ss << obj[fieldId] << " ";
    ss << ")" << std::endl;
  }
}
