//
// Created by liu on 18-10-25.
//

#include "InsertQuery.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../db/Database.h"
#include "../QueryResult.h"

// constexpr const char *InsertQuery::qname;

// example for reference:
// INSERT ( luke 666666666 2015 12 ) FROM Students;
QueryResult::Ptr InsertQuery::execute() {
  using std::literals::string_literals::operator""s;
  if (this->get_operands().empty()) // operands: luke 666666666 2015 12
    return std::make_unique<ErrorMsgResult>(
        qname, this->get_target_table().c_str(),
        "No operand (? operands)."_f % get_operands().size());
  Database &db = Database::getInstance(); // get the database
  try {
    auto &table = db[this->get_target_table()]; // get the table to insert into
    unique_FIFOlock const lock(table.get_table_mutex());
    auto &key =
        this->get_operands().front();   // get the key. in this example: luke
    std::vector<Table::ValueType> data; // prepare a table for values
    data.reserve(this->get_operands().size() -
                 1); // size is for the number of operands except for the key
    for (auto it = ++this->get_operands().begin();
         it != this->get_operands().end(); ++it) {
      data.emplace_back(strtol(it->c_str(), nullptr, 10));
    }
    table.insertByIndex(key, std::move(data));
    return std::make_unique<SuccessMsgResult>(
        qname,
        get_target_table()); // print the results
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

std::string InsertQuery::toString() {
  return "QUERY = INSERT " + this->get_target_table() + "\"";
}
