//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERYBUILDERS_H
#define PROJECT_QUERYBUILDERS_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../db/Table.h"
#include "QueryParser.h"

#define QueryBuilder(name) name##QueryBuilder

#define QueryBuilderClass(name)                                                \
  class QueryBuilder(name) : public QueryBuilder {                             \
    Query::Ptr tryExtractQuery(TokenizedQueryString *query) override;          \
  }

#define BasicQueryBuilderClass(name)                                           \
  class QueryBuilder(name) : public BasicQueryBuilder {                        \
    Query::Ptr tryExtractQuery(TokenizedQueryString *query) override;          \
  }

#define ComplexQueryBuilderClass(name)                                         \
  class QueryBuilder(name) : public ComplexQueryBuilder {                      \
    Query::Ptr tryExtractQuery(TokenizedQueryString *query) override;          \
  }

class FailedQueryBuilder : public QueryBuilder {
public:
  static QueryBuilder::Ptr getDefault() {
    return std::make_unique<FailedQueryBuilder>();
  }

  Query::Ptr tryExtractQuery(TokenizedQueryString *q) final {
    throw QueryBuilderMatchFailed(q->rawQeuryString);
  }

  void setNext(QueryBuilder::Ptr &&builder) final {}

  void clear() override {}

  ~FailedQueryBuilder() override = default;
};

class BasicQueryBuilder : public QueryBuilder {
private:
  QueryBuilder::Ptr nextBuilder;

public:
  void setNext(Ptr &&builder) override { nextBuilder = std::move(builder); }

  QueryBuilder::Ptr &get_nextBuilder() { return nextBuilder; }
  const QueryBuilder::Ptr &get_nextBuilder() const { return nextBuilder; }

  Query::Ptr tryExtractQuery(TokenizedQueryString *query) override {
    return nextBuilder->tryExtractQuery(query);
  }

  BasicQueryBuilder() : nextBuilder(FailedQueryBuilder::getDefault()) {}

  void clear() override { nextBuilder->clear(); }

  ~BasicQueryBuilder() override = default;
};

class ComplexQueryBuilder : public BasicQueryBuilder {
private:
  std::string targetTable;
  std::vector<std::string> operandToken;
  std::vector<QueryCondition> conditionToken;

protected:
  virtual void parseToken(TokenizedQueryString *query);

public:
  std::string &get_target_table() { return targetTable; }
  const std::string &get_target_table() const { return targetTable; }
  std::vector<std::string> &get_operandToken() { return operandToken; }
  const std::vector<std::string> &get_operandToken() const {
    return operandToken;
  }
  std::vector<QueryCondition> &get_conditionToken() { return conditionToken; }
  const std::vector<QueryCondition> &get_conditionToken() const {
    return conditionToken;
  }
  void clear() override;

  // Used as a debugging function.
  // Prints the parsed information
  Query::Ptr tryExtractQuery(TokenizedQueryString *query) override;
};

// Transparant builder
// It does not modify or extract anything
// It prints current tokenized string
// Use to examine the queries and tokenizer
BasicQueryBuilderClass(Fake);

// Debug commands / Utils
BasicQueryBuilderClass(Debug);

// Load, dump, truncate and delete table
BasicQueryBuilderClass(ManageTable);

// ComplexQueryBuilderClass(UpdateTable);
// ComplexQueryBuilderClass(Insert);
// ComplexQueryBuilderClass(Delete);

#endif // PROJECT_QUERYBUILDERS_H
