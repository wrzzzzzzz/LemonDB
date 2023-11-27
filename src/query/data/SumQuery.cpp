#include "SumQuery.h"
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

// constexpr const char *SumQuery::qname;

void SumQuery::findsum(Table *table,
                       std::vector<Table::FieldIndex> field_indexes, int left,
                       int right) {
  std::vector<Table::ValueType> local_sums;
  auto itbegin = table->origin_begin() + left;
  if (itbegin.deleted())
    ++itbegin;
  if (right == -1) { // till the end of table
    for (unsigned int j = 0; j < field_indexes.size(); j++) {
      int temp = 0;
      // iterate through rows
      for (auto it = itbegin; it != table->end(); ++it) {
        if (this->evalCondition(*it)) { // row meets the condition in WHERE
          temp += (*it)[field_indexes[j]];
        }
      }
      local_sums.push_back(temp);
    }
    std::lock_guard<std::mutex> const lock(mtx);
    this->sum_values.push_back(local_sums);
  } else { // iterate through given lines
    for (unsigned int j = 0; j < field_indexes.size(); j++) {
      int temp = 0;
      for (auto it = itbegin; it < table->origin_begin() + right; ++it) {
        if (this->evalCondition(*it)) { // row meets the condition in WHERE
          temp += (*it)[field_indexes[j]];
        }
      }
      local_sums.push_back(temp);
    }
    std::lock_guard<std::mutex> const lock(mtx);
    this->sum_values.push_back(local_sums);
  }
}

// e.g. SUM ( totalCredit class ) FROM Student;
//  should operate based on given columns, say, fieldnames
QueryResult::Ptr SumQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (this->get_operands().empty()) // operands meant to be: totalCredit class
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "No operand (? operands)."_f % get_operands().size());

  Database &db = Database::getInstance(); // get the database
  try {
    auto &table = db[this->get_target_table()]; // get the table to operate
    shared_FIFOlock const lock(table.get_table_mutex());
    std::vector<Table::FieldIndex> field_indexes; // target field indexes
    field_indexes.reserve(this->get_operands().size());

    // store the indexes inside field_indexes
    // TODO: error handling (string given is not valid field string)
    for (auto it = this->get_operands().begin();
         it != this->get_operands().end(); ++it) {
      if (*it != "KEY") {
        field_indexes.emplace_back(table.getFieldIndex(*it));
      } else {
        // error message. since KEY cannot be compared
      }
    }

    // e.g. SUM ( totalCredit class ) FROM Student;
    // ANSWER = ( 350 6043 )
    auto result = initCondition(table);
    std::vector<Table::ValueType> final_result;
    if (result.second) { // meet the WHERE sentence(?)
      int size = static_cast<int>(table.size());
      auto &tp = ThreadPool::getInstance();
      int const NUMOFTHREADS = static_cast<int>(tp.reserve());
      if (NUMOFTHREADS == 1) {
        std::vector<Table::ValueType> local_sums;
        for (unsigned int j = 0; j < field_indexes.size(); j++) {
          int temp = 0;
          // iterate through rows
          for (auto it = table.begin(); it != table.end(); ++it) {
            if (this->evalCondition(*it)) { // row meets the condition in WHERE
              temp += (*it)[field_indexes[j]];
            }
          }
          local_sums.push_back(temp);
        }
        std::lock_guard<std::mutex> const lock_sum(mtx);
        this->sum_values.push_back(local_sums);
      } else {
        size = size / NUMOFTHREADS;
        std::vector<std::thread> threads;
        for (int i = 0; i < NUMOFTHREADS; i++) {
          if (i == NUMOFTHREADS - 1) {
            threads.push_back(
                std::thread(std::bind(&SumQuery::findsum, this, &table,
                                      field_indexes, i * size, -1)));
          } else {
            assert(i * size <= static_cast<int>(table.size()));
            threads.push_back(std::thread(std::bind(&SumQuery::findsum, this,
                                                    &table, field_indexes,
                                                    i * size, (i + 1) * size)));
          }
        }

        // wait for threads to finish
        for (std::thread &thread : threads) {
          thread.join();
        }
        tp.release();
      }
      // find the total_sums
      for (size_t i = 0; i < this->sum_values[0].size(); i++) { // columns
        int temp = 0;
        for (size_t j = 0; j < this->sum_values.size(); j++) { // rows
          temp += sum_values[j][i];
        }
        final_result.push_back(temp);
      }

    } else {
      for (unsigned int j = 0; j < field_indexes.size(); j++) {
        final_result.push_back(0);
      }
    }
    return std::make_unique<SuccessMsgResult>(
        final_result); // meant to be:  ANSWER = ( 350 6043 )

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

std::string SumQuery::toString() {
  return "QUERY = SUM " + this->get_target_table() + "\"";
}
