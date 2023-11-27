#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "../Query.h"

class SelectQuery : public ComplexQuery {
  static constexpr const char *qname = "SELECT";
  std::vector<Table::FieldIndex> fieldIds;
  std::vector<Table::Object> objs;
  std::mutex mtx;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  void printResult(std::vector<Table::Object> *objs, std::stringstream &ss);

  void addobj(Table *table, int left, int right);
};

#endif // PROJECT_SELECTQUERY_H
