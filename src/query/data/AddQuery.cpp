//
// Created by liu on 18-10-25.
//

#include "AddQuery.h"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "../../db/Database.h"

// constexpr const char *AddQuery::qname;

// e.g. UPDATE ( totalCredit 200 ) FROM Student WHERE ( KEY = Jack_Ma );
// "this" refers to the query(?
QueryResult::Ptr AddQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (this->get_operands().size() < 2) // sample: totalCredit 200
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "Invalid number of operands (? operands)."_f % get_operands().size());
  Database &db = Database::getInstance(); // get the database to operate on
  try {
    Table::SizeType counter = 0;
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
      fields.reserve(size);
      this->fieldId = table.getFieldIndex(this->get_operands()[size - 1]);
      for (size_t i = 0; i < size - 1; i++) {
        fields.push_back(table.getFieldIndex(this->get_operands()[i]));
      }
      for (auto it = table.begin(); it != table.end();
           ++it) { // it -> rows, this -> the query
        if (this->evalCondition(*it)) {
          // if (this->keyValue.empty()) { // there's no Amy, means not "KEY
          // Amy" as operands
          //   (*it)[this->fieldId] = this->fieldValue; // update the value at
          //   corresponding place
          // } else {
          //   it->setKey(this->keyValue); // change the key of the row to Amy
          // }
          // for (auto f : fields) {
          //   resultValue += (*it)[f];
          // }
          (*it)[this->fieldId] =
              std::accumulate(fields.begin(), fields.end(), 0,
                              [&](Table::ValueType a, Table::FieldIndex f) {
                                return a + (*it)[f];
                              });
          ;

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

std::string AddQuery::toString() {
  return "QUERY = ADD " + this->get_target_table() + "\"";
}
