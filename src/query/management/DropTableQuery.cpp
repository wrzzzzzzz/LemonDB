//
// Created by liu on 18-10-25.
//

#include "DropTableQuery.h"

#include <memory>
#include <string>

#include "../../db/Database.h"

// constexpr const char *DropTableQuery::qname;

QueryResult::Ptr DropTableQuery::execute() {
  using std::literals::string_literals::operator""s;
  Database &db = Database::getInstance();
  try {
    db.dropTable(this->get_target_table());
    return std::make_unique<SuccessMsgResult>(qname);
  } catch (const TableNameNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, get_target_table(),
                                            "No such table."s);
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string DropTableQuery::toString() {
  return "QUERY = DROP, Table = \"" + get_target_table() + "\"";
}
