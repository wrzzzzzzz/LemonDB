//
// Created on 23-10-23.
//

#ifndef PROJECT_SWAPQUERY_H
#define PROJECT_SWAPQUERY_H

#include <atomic>
#include <string>

#include "../Query.h"

class SwapQuery : public ComplexQuery {
  static constexpr const char *qname = "SWAP";
  std::atomic<int> okCount = 0;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;

  // Change the function prototype in SwapQuery.h to:
  void swapFields(Table *table, const std::string &field1,
                  const std::string &field2, int firstRow, int lastRow);
};

#endif // PROJECT_SWAPQUERY_H
