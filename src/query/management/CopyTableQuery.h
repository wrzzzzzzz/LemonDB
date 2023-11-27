//
// Created by luoyi_fan on 23-10-21.
//

#ifndef PROJECT_COPYTABLEQUERY_H
#define PROJECT_COPYTABLEQUERY_H

#include <string>
#include <utility>

#include "../Query.h"

class CopyTableQuery : public Query {
  static constexpr const char *qname = "COPY";
  const std::string tableName;

public:
  CopyTableQuery(std::string table, std::string tableName)
      : Query(std::move(table)), tableName(std::move(tableName)) {}

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_COPYTABLEQUERY_H
