//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include <string>

#include "../Query.h"

class AddQuery : public ComplexQuery {
  static constexpr const char *qname = "ADD";
  // Table::ValueType
  //     fieldValue; // = (operands[0]=="KEY")? 0 :std::stoi(operands[1]);
  Table::FieldIndex fieldId = 0;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_ADDQUERY_H
