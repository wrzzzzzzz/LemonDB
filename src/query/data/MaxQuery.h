#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "../Query.h"

class MaxQuery : public ComplexQuery {
  static constexpr const char *qname = "MAX";
  Table::KeyType keyValue;
  std::vector<std::vector<Table::ValueType>>
      values; // to store rows of local minimum values from patches
  std::mutex mtx;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  void findmax(Table *table, std::vector<Table::FieldIndex> field_indexes,
               int left, int right);
};

#endif // PROJECT_MAXQUERY_H
