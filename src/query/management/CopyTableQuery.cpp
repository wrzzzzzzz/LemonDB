//
// Created by luoyi_fan on 23-10-21.
//

#include "CopyTableQuery.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "../../db/Database.h"

// constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute() {
  auto &db = Database::getInstance();
  try {
    auto &table = db[this->get_target_table()];
    std::unique_lock<FIFOSharedMutex> const lock2(table.get_table_mutex());
    unique_FIFOlock const lock(db.get_database_mutex());
    Table::Ptr newTable = std::make_unique<Table>(this->tableName, table);
    db.registerTable(std::move(newTable));
    return std::make_unique<SuccessMsgResult>(qname, get_target_table(),
                                              "Copied to " + this->tableName);
  } catch (const std::exception &e) {
    return std::make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string CopyTableQuery::toString() {
  return "QUERY = Copy TABLE, NEW TABLE = \"" + this->tableName + "\"";
}
