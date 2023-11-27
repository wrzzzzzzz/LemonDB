#ifndef PROJECT_SUBTRACTQUERY_H
#define PROJECT_SUBTRACTQUERY_H

#include <atomic> // <-- This is added to use std::atomic
#include <string>
#include <vector>

#include "../Query.h"

class SubQuery : public ComplexQuery {
  static constexpr const char *qname = "SUB";
  // Table::ValueType
  //     fieldValue; // = (operands[0]=="KEY")? 0 :std::stoi(operands[1]);
  Table::FieldIndex fieldId = 0;
  std::atomic<int> counter = 0;
  // <-- This replaces the "int counter" to be thread-safe

public:
  using ComplexQuery::ComplexQuery;

  // This function will be executed by each thread
  void subtractFields(Table *table, int firstRow, int lastRow,
                      const std::vector<Table::FieldIndex> &fields);

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_SUBTRACTQUERY_H
