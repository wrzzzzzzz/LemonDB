#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "../Query.h"

class CountQuery : public ComplexQuery {
  static constexpr const char *qname = "COUNT";
  Table::KeyType keyValue;
  std::vector<size_t> sizes; // vector to store sizes of patches
  std::mutex mtx;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  void findsize(Table *table, int left, int right);
};

#endif // PROJECT_COUNTQUERY_H
