#include "DuplicateQuery.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../utils/ThreadPool.h"
#include "../QueryResult.h"

// constexpr const char *DuplicateQuery::qname;

void DuplicateQuery::addelement(Table *table, int left, int right, int flag) {
  auto it = table->origin_begin() + left;
  if (it.deleted())
    ++it;
  if (right == -1) {
    for (; it < table->end(); ++it) {
      if (this->evalCondition(*it) && !table->duplicateEval(it->key())) {
        Table::KeyType key = it->key() + "_copy";
        std::lock_guard<std::mutex> const lock(mtx);
        keys.push_back(std::make_pair(flag, std::move(key)));
        values.push_back(std::make_pair(flag, it->value()));
        affect++;
      }
    }
  } else {
    for (; it < table->origin_begin() + right; ++it) {
      if (this->evalCondition(*it) && !table->duplicateEval(it->key())) {
        Table::KeyType key = it->key() + "_copy";
        std::lock_guard<std::mutex> const lock(mtx);
        // if (flag == 0) {
        //   std::cout << key << " ";
        // }
        keys.push_back(std::make_pair(flag, std::move(key)));
        values.push_back(std::make_pair(flag, it->value()));
        affect++;
      }
    }
  }
}

QueryResult::Ptr DuplicateQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (!this->get_operands().empty())
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Invalid number of operands (? operands) or without leading KEY."_f %
            get_operands().size());
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    unique_FIFOlock const lock(table.get_table_mutex());
    auto result = initCondition(table);
    if (result.second) {
      int size = static_cast<int>(table.size());
      auto &tp = ThreadPool::getInstance();
      int const NUMOFTHREADS = static_cast<int>(tp.reserve());
      if (NUMOFTHREADS == 1) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it) && !table.duplicateEval(it->key())) {
            Table::KeyType key = it->key() + "_copy";
            keys.push_back(std::make_pair(1, std::move(key)));
            values.push_back(std::make_pair(1, it->value()));
            affect++;
          }
        }
      } else {
        size = size / NUMOFTHREADS;
        std::vector<std::thread> threads;
        for (int i = 0; i < NUMOFTHREADS; ++i) {
          if (i == NUMOFTHREADS - 1) {
            threads.push_back(std::thread(std::bind(
                &DuplicateQuery::addelement, this, &table, i * size, -1, i)));
          } else {
            assert(i * size <= static_cast<int>(table.size()));
            threads.push_back(
                std::thread(std::bind(&DuplicateQuery::addelement, this, &table,
                                      i * size, (i + 1) * size, i)));
          }
        }
        for (std::thread &thread : threads) {
          thread.join();
        }
        tp.release();
      }
    }
    // std::cout << "-----------------------------------------" << std::endl;
    // std::vector<size_t> indices(keys.size());
    // for (size_t i = 0; i < keys.size(); ++i) {
    //     indices[i] = i;
    // }
    // std::sort(indices.begin(), indices.end(), [this](size_t a, size_t b) {
    //       return keys[a].first < keys[b].first;
    // });
    std::stable_sort(keys.begin(), keys.end(),
                     [](const std::pair<int, Table::KeyType> &k1,
                        const std::pair<int, Table::KeyType> &k2) {
                       return k1.first < k2.first;
                     });
    std::stable_sort(
        values.begin(), values.end(),
        [](const std::pair<int, std::vector<Table::ValueType>> &k1,
           const std::pair<int, std::vector<Table::ValueType>> &k2) {
          return k1.first < k2.first;
        });
    // for (const auto& key : keys) {
    //   if (key.first == 0)
    //     std::cout << key.second << " ";
    // }
    // for (const size_t index : indices) {
    //   const auto& item = keys[index];
    //   std::cout << item.first << ": " << item.second << std::endl;
    // }
    // std::cout << std::endl;
    for (Table::SizeType i = 0; i < affect; ++i) {
      table.insertByIndex(keys[i].second, std::move(values[i].second));
    }
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

std::string DuplicateQuery::toString() {
  return "QUERY = DUPLICATE " + this->get_target_table() + "\"";
}
