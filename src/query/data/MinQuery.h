#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "../Query.h"

class MinQuery : public ComplexQuery {
  static constexpr const char *qname = "MIN";
  Table::KeyType keyValue;
  std::vector<std::vector<Table::ValueType>>
      values; // to store rows of local minimum values from patches
  std::mutex mtx;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  void findmin(Table *table, std::vector<Table::FieldIndex> field_indexes,
               int left, int right);
};

#endif // PROJECT_MINQUERY_H
