//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERYRESULT_H
#define PROJECT_QUERYRESULT_H

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../utils/formatter.h"

class QueryResult {
public:
  typedef std::unique_ptr<QueryResult> Ptr;

  virtual bool success() = 0;

  virtual bool display() const = 0;

  virtual ~QueryResult() = default;

  friend std::ostream &operator<<(std::ostream &os, const QueryResult &table);

  virtual std::ostream &output_err(std::ostream &os) const {
    return output(os);
  }

protected:
  virtual std::ostream &output(std::ostream &os) const = 0;
};

class FailedQueryResult : public QueryResult {
  const std::string data;

public:
  bool success() override { return false; }

  bool display() const override { return false; }
};

class SucceededQueryResult : public QueryResult {
public:
  bool success() override { return true; }
};

class NullQueryResult : public SucceededQueryResult {
public:
  bool display() const override { return false; }

protected:
  std::ostream &output(std::ostream &os) const override { return os << "\n"; }
};

class ErrorMsgResult : public FailedQueryResult {
  std::string msg;

public:
  bool display() const override { return false; }

  ErrorMsgResult(const char *qname, const std::string &msg) {
    this->msg = R"(Query "?" failed : ?)"_f % qname % msg.c_str();
  }

  ErrorMsgResult(const char *qname, const std::string &table,
                 const std::string &msg) {
    this->msg = R"(Query "?" failed in Table "?" : ?)"_f % qname %
                table.c_str() % msg.c_str();
  }

protected:
  std::ostream &output(std::ostream &os) const override {
    return os << msg << "\n";
  }
};

class SuccessMsgResult : public SucceededQueryResult {
  std::string msg;

public:
  bool display() const override { return this->displayResult; }
  void setDisplayResult(bool val) { this->displayResult = val; }

  explicit SuccessMsgResult(const int number) {
    this->msg = R"(ANSWER = ?)"_f % number;
    displayResult = true;
  }

  explicit SuccessMsgResult(const std::vector<int> &results) {
    std::stringstream ss;
    ss << "ANSWER = ( ";
    for (auto result : results) {
      ss << result << " ";
    }
    ss << ")";
    this->msg = ss.str();
    displayResult = true;
  }

  explicit SuccessMsgResult(const char *qname) {
    this->msg = R"(Query "?" success.)"_f % qname;
  }

  SuccessMsgResult(const char *qname, const std::string &msg) {
    this->msg = R"(Query "?" success : ?)"_f % qname % msg.c_str();
  }

  SuccessMsgResult(const char *qname, const std::string &table,
                   const std::string &msg) {
    this->msg = R"(Query "?" success in Table "?" : ?)"_f % qname %
                table.c_str() % msg.c_str();
  }

protected:
  // bool displayResult = false;
  std::ostream &output(std::ostream &os) const override {
    return os << msg << "\n";
  }

private:
  bool displayResult = false;
};

class RecordCountResult : public SucceededQueryResult {
  const int affectedRows;

public:
  bool display() const override { return true; }

  explicit RecordCountResult(int count) : affectedRows(count) {}

protected:
  std::ostream &output(std::ostream &os) const override {
    return os << "Affected ? rows."_f % affectedRows << "\n";
  }
};

class CustomizedResult : public SuccessMsgResult {
  std::stringstream ss;

public:
  explicit CustomizedResult(const char *qname, const std::string &msg,
                            std::stringstream &&ss)
      : SuccessMsgResult(qname, msg), ss(std::move(ss)) {
    // displayResult = true;
    setDisplayResult(true);
  }

protected:
  std::ostream &output(std::ostream &os) const override {
    return os << ss.str();
  }
  std::ostream &output_err(std::ostream &os) const override {
    return SuccessMsgResult::output(os);
  }
};

#endif // PROJECT_QUERYRESULT_H
