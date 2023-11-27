#include "SwapQuery.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../../utils/ThreadPool.h"
#include "../QueryResult.h"

// constexpr const char *SwapQuery::qname;

// single thread
/*
QueryResult::Ptr SwapQuery::execute() {
  using namespace std;

  if (this->operands.size() != 2)
    return make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Wrong number of operands (% operands)."_f % operands.size());
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    unique_FIFOlock const lock(table.get_table_mutex());
    auto ok_count = 0;
    auto field1 = this->operands[0];
    auto field2 = this->operands[1];

    auto result = initCondition(table);
    if (result.second) {
      for (auto it = table.begin(); it != table.end(); ++it) {
        if (this->evalCondition(*it) == true) {
          // Debug statement to print which row is being swapped
          if (field1 != field2) {
            std::swap((*it)[field1], (*it)[field2]);
          }
          ++ok_count;
        }
      }
    }

    // Return results
    return make_unique<RecordCountResult>(ok_count);
  } catch (const TableNameNotFound &e) {
    return make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                       "No such table."s);
  } catch (const IllFormedQueryCondition &e) {
    return make_unique<ErrorMsgResult>(qname, this->get_target_table(),
e.what()); } catch (const invalid_argument &e) {
    // Cannot convert operand to string
    return make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                       "Unknown error '?'"_f % e.what());
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                       "Unkonwn error '?'."_f % e.what());
  }
}
*/

// multithread
/* bad strategy to iterate through
void SwapQuery::swapFields(Table& table, const std::string& field1, const
std::string& field2, int firstRow, int lastRow, std::atomic<int>& okCount) {
    auto rowIt = table.begin();
    for(int i = 0; i < firstRow; i++)
        ++rowIt;
    auto endIt = table.begin();
    for(int i = 0; i < lastRow; i++)
        ++endIt;
    for (; rowIt != endIt; ++rowIt) {
        if (this->evalCondition(*rowIt) == true) {
            std::swap((*rowIt)[field1], (*rowIt)[field2]);
            ++okCount;
        }
    }
}
*/

void SwapQuery::swapFields(Table *table, const std::string &field1,
                           const std::string &field2, int firstRow,
                           int lastRow) {
  auto rowIt = table->origin_begin() + firstRow;
  if (rowIt.deleted())
    ++rowIt;
  auto endIt =
      (lastRow == -1) ? table->end() : (table->origin_begin() + lastRow);
  for (; rowIt < endIt; ++rowIt) {
    if (this->evalCondition(*rowIt) == true) {
      if (field1 != field2) {
        std::swap((*rowIt)[field1], (*rowIt)[field2]);
      }
      ++okCount;
    }
  }
}

QueryResult::Ptr SwapQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (this->get_operands().size() != 2)
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Wrong number of operands (% operands)."_f % get_operands().size());
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    unique_FIFOlock const lock(table.get_table_mutex());
    auto field1 = this->get_operands()[0];
    auto field2 = this->get_operands()[1];

    // if (field1 == field2) {
    //     return std::make_unique<SuccessMsgResult>(qname, get_target_table(),
    //     "Nothing to swap."_f);
    // }

    auto result = initCondition(table);
    if (result.second) {

      auto &tp = ThreadPool::getInstance();
      int const numThreads = static_cast<int>(tp.reserve());
      if (numThreads == 1) {
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it) == true) {
            // Debug statement to print which row is being swapped
            if (field1 != field2) {
              std::swap((*it)[field1], (*it)[field2]);
            }
            ++okCount;
          }
        }
      } else {
        // Compute the number of rows
        int const numRows = static_cast<int>(table.size());
        const int rowsPerThread = numRows / numThreads;
        std::vector<std::thread> threads;
        for (int i = 0; i < numThreads; ++i) {
          int firstRow = i * rowsPerThread;
          // The last thread should process all remaining rows
          int lastRow = (i == numThreads - 1) ? -1 : (i + 1) * rowsPerThread;
          threads.push_back(std::thread(&SwapQuery::swapFields, this, &table,
                                        field1, field2, firstRow, lastRow));
        }

        for (std::thread &thread : threads) {
          thread.join();
        }
        tp.release();
      }
    }

    return std::make_unique<RecordCountResult>(okCount);
  } catch (const TableNameNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            "No such table."s);
  } catch (const IllFormedQueryCondition &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            e.what());
  } catch (const std::invalid_argument &e) {
    // The operand cannot be converted to a string
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            "Unknown error '?'"_f % e.what());
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            "Unknown error '?'."_f % e.what());
  }
}

std::string SwapQuery::toString() {
  return "QUERY = SWAP " + this->get_target_table() + "\"";
}
