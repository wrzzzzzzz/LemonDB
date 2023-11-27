#include "SubtractQuery.h"

#include <iostream> // <-- This is added to use std::cout
#include <memory>
#include <mutex> // <-- To solve race condition of counter
#include <string>
#include <thread> // <-- This is added to use std::thread
#include <vector> // <-- This is added to use std::vector

#include "../../db/Database.h"
#include "../../utils/ThreadPool.h"

// constexpr const char *SubQuery::qname;

// std::mutex mtx;

void SubQuery::subtractFields(Table *table, int firstRow, int lastRow,
                              const std::vector<Table::FieldIndex> &fields) {
  auto it = table->origin_begin() + firstRow;
  if (it.deleted())
    ++it;
  auto end = (lastRow == -1) ? table->end() : (table->origin_begin() + lastRow);
  for (; it < end; ++it) {
    if (this->evalCondition(*it)) {
      Table::ValueType result = (*it)[fields[0]]; // Start from the first field
      for (size_t i = 1; i < fields.size();
           ++i) { // Then subtract all other fields
        result -= (*it)[fields[i]];
      }
      (*it)[this->fieldId] = result;
      // std::lock_guard<std::mutex> const guard(mtx);  // lock until the end of
      // the current scope
      ++counter;
    }
  }
}

// e.g. UPDATE ( totalCredit 200 ) FROM Student WHERE ( KEY = Jack_Ma );
// "this" refers to the query(?
QueryResult::Ptr SubQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (this->get_operands().size() < 2) // sample: totalCredit 200
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Invalid number of operands (? operands)."_f % get_operands().size());
  Database &db = Database::getInstance(); // get the database to operate on
  counter = 0;
  // Table::SizeType counter = 0; single thread counter

  try {
    auto &table =
        db[this->get_target_table()]; // get the target table to operate on
    unique_FIFOlock const lock(table.get_table_mutex());
    // if (this->operands[0] == "KEY") { // e.g. if it's ( KEY Amy )
    //   this->keyValue = this->operands[1]; // store Amy
    // } else {
    // this->fieldId = table.getFieldIndex(this->operands[0]); // get the index
    // of totalCredit
    //   this->fieldValue =
    //       (Table::ValueType)strtol(this->operands[1].c_str(), nullptr, 10);
    //       // get the value 200 as int
    // }
    auto result = initCondition(table);
    if (result.second) {
      size_t const size = this->get_operands().size();
      std::vector<Table::FieldIndex> fields;
      fields.reserve(size - 1);
      this->fieldId = table.getFieldIndex(this->get_operands()[size - 1]);
      for (size_t i = 0; i < size - 1; i++) {
        fields.push_back(table.getFieldIndex(this->get_operands()[i]));
      }

      // multithread
      // THE MULTITHREADING PART STARTS HERE!
      // number of threads
      auto &tp = ThreadPool::getInstance();
      size_t const numThreads = tp.reserve();
      if (numThreads == 1) {
        for (auto it = table.begin(); it < table.end(); ++it) {
          if (this->evalCondition(*it)) {
            Table::ValueType resultValue =
                (*it)[fields[0]]; // Start from the first field
            for (size_t i = 1; i < fields.size();
                 ++i) { // Then subtract all other fields
              resultValue -= (*it)[fields[i]];
            }
            (*it)[this->fieldId] = resultValue;
            // std::lock_guard<std::mutex> const guard(mtx);  // lock until the
            // end of the current scope
            ++counter;
          }
        }
      } else {
        std::vector<std::thread> threads; // vector of threads
        // std::vector<Table::Iterator> iterators(numThreads + 1);  // vector of
        // iterators

        size_t const rowNums = table.size();
        // std::cout << "Total rows of the table: " << rowNums << std::endl;

        size_t const rowsPerThread = rowNums / numThreads;
        // auto it = table.begin();

        // const unsigned int remainder = rowNums % numThreads;
        // for (unsigned int i = 0; i < numThreads; ++i) {
        //     iterators[i] = it;
        //     const unsigned int rowsForThisThread = rowsPerThread + (i <
        //     remainder ? 1 : 0);
        //     // std::cout << "Thread " << i << " is responsible for " <<
        //     rowsForThisThread << " rows." << std::endl; for(unsigned int j =
        //     0; j < rowsForThisThread; ++j) {
        //         ++it;
        //     }
        // }
        // iterators[numThreads] = it;

        // for (unsigned int i = 0; i < numThreads; ++i) {
        //   threads[i] = std::thread(&SubQuery::subtractFields, this,
        //   std::ref(iterators[i]), std::ref(iterators[i+1]),
        //   std::ref(fields));
        // }
        for (size_t i = 0; i < numThreads; ++i) {
          int firstRow = static_cast<int>(i * rowsPerThread);
          // The last thread should process all remaining rows
          int lastRow = (i == numThreads - 1)
                            ? -1
                            : static_cast<int>((i + 1) * rowsPerThread);
          threads.push_back(std::thread(&SubQuery::subtractFields, this, &table,
                                        firstRow, lastRow, std::ref(fields)));
        }

        // Join the threads
        for (auto &th : threads) {
          th.join();
        }
        tp.release();
        // END OF MULTITHREADING
      }
    }
    return std::make_unique<RecordCountResult>(counter);
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

std::string SubQuery::toString() {
  return "QUERY = SUBTRACT " + this->get_target_table() + "\"";
}
