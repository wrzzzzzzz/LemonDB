//
// Created by liu on 18-10-25.
//

#include "PrintTableQuery.h"

#include <iostream>
#include <memory>
#include <string>

#include "../../db/Database.h"

// constexpr const char *PrintTableQuery::qname;

QueryResult::Ptr PrintTableQuery::execute() {
  using std::literals::string_literals::operator""s;
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    shared_FIFOlock const lock(table.get_table_mutex());
    std::cout << "================\n";
    std::cout << "TABLE = ";
    std::cout << table;
    std::cout << "================\n" << std::endl;
    return std::make_unique<SuccessMsgResult>(qname, this->get_target_table());
  } catch (const TableNameNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, this->get_target_table(),
                                            "No such table."s);
  }
}

std::string PrintTableQuery::toString() {
  return "QUERY = SHOWTABLE, Table = \"" + this->get_target_table() + "\"";
}
