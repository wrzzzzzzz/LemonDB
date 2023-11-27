//
// Created by liu on 18-10-25.
//

#include "UpdateQuery.h"

#include <memory>
#include <string>

#include "../../db/Database.h"

// constexpr const char *UpdateQuery::qname;

// e.g. UPDATE ( totalCredit 200 ) FROM Student WHERE ( KEY = Jack_Ma );
// "this" refers to the query(?
QueryResult::Ptr UpdateQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (this->get_operands().size() != 2) // sample: totalCredit 200
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Invalid number of operands (? operands)."_f % get_operands().size());
  Database &db = Database::getInstance(); // get the database to operate on
  try {
    Table::SizeType counter = 0;
    auto &table =
        db[this->get_target_table()]; // get the target table to operate on
    unique_FIFOlock const lock(table.get_table_mutex());
    if (this->get_operands()[0] == "KEY") {     // e.g. if it's ( KEY Amy )
      this->keyValue = this->get_operands()[1]; // store Amy
    } else {
      this->fieldId = table.getFieldIndex(
          this->get_operands()[0]); // get the index of totalCredit
      this->fieldValue =
          (Table::ValueType)strtol(this->get_operands()[1].c_str(), nullptr,
                                   10); // get the value 200 as int
    }
    auto result = initCondition(table);
    if (result.second) {
      for (auto it = table.begin(); it != table.end();
           ++it) { // it -> rows, this -> the query
        if (this->evalCondition(*it)) {
          if (this->keyValue
                  .empty()) { // there's no Amy, means not "KEY Amy" as operands
            (*it)[this->fieldId] =
                this->fieldValue; // update the value at corresponding place
          } else {
            it->setKey(this->keyValue); // change the key of the row to Amy
          }
          ++counter;
        }
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

std::string UpdateQuery::toString() {
  return "QUERY = UPDATE " + this->get_target_table() + "\"";
}
