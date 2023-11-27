#ifndef PROJECT_SUMQUERY_H
#define PROJECT_SUMQUERY_H

#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "../Query.h"

class SumQuery : public ComplexQuery {
  static constexpr const char *qname = "SUM";
  Table::KeyType keyValue;
  std::vector<std::vector<Table::ValueType>> sum_values;
  std::mutex mtx;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  void findsum(Table *table, std::vector<Table::FieldIndex> field_indexes,
               int left, int right);
};

#endif // PROJECT_SUMQUERY_H
