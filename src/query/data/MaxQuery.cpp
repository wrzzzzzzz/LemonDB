#include "MaxQuery.h"

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

// constexpr const char *MaxQuery::qname;

// the function that finds local maximums of 20 rows, i.e.
// local_maximums: the vector of maximum values over the 20 lines i.e., (112
// 2014) values: a vector of the rows of local maximums, i.e., 2 3 4 5 (do we
// create a table type or create an array of ints?)
void MaxQuery::findmax(Table *table,
                       std::vector<Table::FieldIndex> field_indexes, int left,
                       int right = -1) {
  std::vector<Table::ValueType> local_maximums;
  auto itbegin = table->origin_begin() + left;
  if (itbegin.deleted())
    ++itbegin;
  if (right == -1) { // till the end of table
    for (unsigned int j = 0; j < field_indexes.size(); j++) {
      int temp = table->ValueTypeMin;
      int flag = 0;
      // iterate through rows
      for (auto it = itbegin; it != table->end(); ++it) {
        if (this->evalCondition(*it)) { // row meets the condition in WHERE
          if ((*it)[field_indexes[j]] > temp) {
            flag = 1;
            temp = (*it)[field_indexes[j]];
          }
        }
      }
      if (flag == 1) {
        local_maximums.push_back(temp);
      }
    }
    std::lock_guard<std::mutex> const lock(mtx);
    if (local_maximums.size() != 0) {
      this->values.push_back(local_maximums);
    }
  } else { // iterate through given lines
    for (unsigned int j = 0; j < field_indexes.size(); j++) {
      int temp = table->ValueTypeMin;
      int flag = 0;
      for (auto it = itbegin; it < table->origin_begin() + right; ++it) {
        if (this->evalCondition(*it)) { // row meets the condition in WHERE
          if ((*it)[field_indexes[j]] > temp) {
            flag = 1;
            temp = (*it)[field_indexes[j]];
          }
        }
      }
      if (flag == 1) {
        local_maximums.push_back(temp);
      }
    }
    std::lock_guard<std::mutex> const lock(mtx);
    if (local_maximums.size() != 0) {
      this->values.push_back(local_maximums);
    }
  }
}

// e.g. MAX ( totalCredit class ) FROM Student WHERE ( class > 2014 );
//  should operate based on given columns, say, fieldnames
QueryResult::Ptr MaxQuery::execute() {
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

    // e.g. MAX ( totalCredit class ) FROM Student WHERE ( class > 2013 );
    // sample output: ANSWER = ( 112 2014 )
    auto result = initCondition(table);
    std::vector<Table::ValueType> final_result;
    if (result.second) { // meet the WHERE sentence(?)
      int size = static_cast<int>(table.size());
      auto &tp = ThreadPool::getInstance();
      int const NUMOFTHREADS = static_cast<int>(tp.reserve());
      if (NUMOFTHREADS == 1) {
        for (unsigned int j = 0; j < field_indexes.size(); j++) {
          int temp = table.ValueTypeMin;
          int flag = 0;
          // iterate through rows
          for (auto it = table.begin(); it != table.end(); ++it) {
            if (this->evalCondition(*it)) { // row meets the condition in WHERE
              if ((*it)[field_indexes[j]] > temp) {
                flag = 1;
                temp = (*it)[field_indexes[j]];
              }
            }
          }
          if (flag == 1) {
            final_result.push_back(temp);
          }
        }
      } else {
        size = size / NUMOFTHREADS;
        std::vector<std::thread> threads;
        for (int i = 0; i < NUMOFTHREADS; i++) {
          if (i == NUMOFTHREADS - 1) {
            threads.push_back(
                std::thread(std::bind(&MaxQuery::findmax, this, &table,
                                      field_indexes, i * size, -1)));
          } else {
            assert(i * size <= static_cast<int>(table.size()));
            threads.push_back(std::thread(std::bind(&MaxQuery::findmax, this,
                                                    &table, field_indexes,
                                                    i * size, (i + 1) * size)));
          }
        }

        // wait for threads to finish
        for (std::thread &thread : threads) {
          thread.join();
        }

        // find the maxima of each column
        if (this->values.size() != 0) {
          for (size_t i = 0; i < this->values[0].size(); i++) { // columns
            int temp = table.ValueTypeMin;
            for (size_t j = 0; j < this->values.size(); j++) { // rows
              if (values[j][i] > temp) {
                temp = values[j][i];
              }
            }
            final_result.push_back(temp);
          }
        }
        tp.release();
      }
    }
    if (final_result.size() == 0) {
      return std::make_unique<NullQueryResult>();
    }
    return std::make_unique<SuccessMsgResult>(
        final_result); // meant to be: ANSWER = ( 112 2014 )

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

std::string MaxQuery::toString() {
  return "QUERY = MAX " + this->get_target_table() + "\"";
}
