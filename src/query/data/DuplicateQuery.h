#ifndef PROJECT_DUPLICATEQUERY_H
#define PROJECT_DUPLICATEQUERY_H

#include <cassert>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../Query.h"

class DuplicateQuery : public ComplexQuery {
  static constexpr const char *qname = "DUPLICATE";
  std::mutex mtx;
  std::vector<std::pair<int, Table::KeyType>> keys;
  std::vector<std::pair<int, std::vector<Table::ValueType>>> values;
  Table::SizeType affect = 0;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  void addelement(Table *table, int left, int right, int flag);

  // Table::SizeType duplicateInsert(Table& table, std::vector<Table::Object>
  // &objs);
};

#endif // PROJECT_DUPLICATEQUERY_H
