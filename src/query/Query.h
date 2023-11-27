//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERY_H
#define PROJECT_QUERY_H

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "../db/Table.h"
#include "QueryResult.h"
// #include "src/utils/ThreadPool.h"

struct QueryCondition {
  // rz: e.g. UPDATE ( totalCredit 200 ) FROM Student WHERE ( KEY = Jack_Ma );
  std::string field; // KEY
  size_t fieldId;    // 0
  std::string op;    //=
  std::function<bool(const Table::ValueType &, const Table::ValueType &)>
      comp;          // KEY = Jack_Ma?
  std::string value; // Jack_Ma?
  Table::ValueType valueParsed;
};

class Query {
private:
  std::string targetTable; // the name of the table being operated on

public:
  std::string &get_target_table() { return targetTable; }
  const std::string &get_target_table() const { return targetTable; }
  Query() = default;

  explicit Query(std::string targetTable)
      : targetTable(std::move(targetTable)) {}

  typedef std::unique_ptr<Query> Ptr;

  virtual QueryResult::Ptr execute() = 0;

  virtual std::string toString() = 0;

  virtual ~Query() = default; // destructor
};

class NopQuery : public Query {
public:
  QueryResult::Ptr execute() override {
    return std::make_unique<NullQueryResult>();
  }

  std::string toString() override { return "QUERY = NOOP"; }
};

class ComplexQuery : public Query {
private:
  /** The field names in the first () */
  std::vector<std::string> operands;
  /** The function used in where clause */
  std::vector<QueryCondition>
      condition; // e.g. WHERE ( totalCredit > 100 ) ( class < 2015 ):
                 // several query conditions. each one as an element in the
                 // vector
public:
  std::vector<std::string> &get_operands() { return operands; }
  const std::vector<std::string> &get_operands() const { return operands; }
  std::vector<QueryCondition> &get_condition() { return condition; }
  const std::vector<QueryCondition> &get_condition() const { return condition; }
  typedef std::unique_ptr<ComplexQuery> Ptr;

  /**
   * init a fast condition according to the table
   * note that the condition is only effective if the table fields are not
   * changed
   * @param table
   * @param conditions
   * @return a pair of the key and a flag
   * if flag is false, the condition is always false
   * in this situation, the condition may not be fully initialized to save time
   */
  std::pair<std::string, bool> initCondition(const Table &table);

  /**
   * skip the evaluation of KEY
   * (which should be done after initConditionFast is called)
   * @param conditions
   * @param object
   * @return
   */
  bool evalCondition(const Table::Object &object);

  /**
   * This function seems have small effect and causes somme bugs
   * so it is not used actually
   * @param table
   * @param function
   * @return
   */
  // bool testKeyCondition(
  //     // change due to code quality
  //     const Table &table,
  //     const std::function<void(bool, Table::Object::Ptr &&)> &function);

  ComplexQuery(std::string targetTable, std::vector<std::string> operands,
               std::vector<QueryCondition> condition)
      : Query(std::move(targetTable)), operands(std::move(operands)),
        condition(std::move(condition)) {}

  /** Get operands in the query */
  const std::vector<std::string> &getOperands() const { return operands; }

  /** Get condition in the query, seems no use now */
  const std::vector<QueryCondition> &getCondition() { return condition; }
};

#endif // PROJECT_QUERY_H
