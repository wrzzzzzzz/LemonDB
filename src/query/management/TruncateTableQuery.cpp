//
// Created by liu on 18-10-25.
//

#include "TruncateTableQuery.h"
#include <memory>
#include <string>

#include "../../db/Database.h"
// constexpr const char *TruncateTableQuery::qname;

QueryResult::Ptr TruncateTableQuery::execute() {
  using std::literals::string_literals::operator""s;
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    unique_FIFOlock const lock(table.get_table_mutex());
    table.clear();
    return std::make_unique<SuccessMsgResult>(qname);
  } catch (const TableNameNotFound &e) {
    return std::make_unique<ErrorMsgResult>(qname, get_target_table(),
                                            "No such table."s);
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string TruncateTableQuery::toString() {
  return "QUERY = TRUNCATE, Table = \"" + get_target_table() + "\"";
}
