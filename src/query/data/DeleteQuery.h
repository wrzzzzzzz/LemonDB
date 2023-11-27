#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H

#include <cassert>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../Query.h"

class DeleteQuery : public ComplexQuery {
  static constexpr const char *qname = "DELETE";
  // Table::FieldIndex fieldId;
  //   std::vector<Table::FieldIndex> fieldIds;
  // Table::KeyType keyValue;
  std::vector<Table::KeyType> keys;
  std::mutex mtx;
  Table::SizeType affect = 0;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  void addelement(Table *table, int left, int right);
};

#endif // PROJECT_DELETEQUERY_H
