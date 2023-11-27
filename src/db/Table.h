//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_TABLE_H
#define PROJECT_DB_TABLE_H

#include <algorithm>
#include <climits>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../utils/FIFOSharedMutex.h"
#include "../utils/formatter.h"
#include "../utils/uexception.h"

#define _DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field)                             \
  do {                                                                         \
    try {                                                                      \
      auto &index = table->fieldMap.at(field);                                 \
      return it->getDatum().at(index);                                         \
    } catch (const std::out_of_range &e) {                                     \
      throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f %         \
                               (field.c_str()));                               \
    }                                                                          \
  } while (0)

#define _DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index)                            \
  do {                                                                         \
    try {                                                                      \
      return it->getDatum().at(index);                                         \
    } catch (const std::out_of_range &e) {                                     \
      throw TableFieldNotFound(R"(Field index ? out of range.)"_f % (index));  \
    }                                                                          \
  } while (0)

class Table {
public:
  typedef std::string KeyType;
  typedef std::string FieldNameType;
  typedef size_t FieldIndex;
  typedef int ValueType; // values stored in the table are integers
  static constexpr const ValueType ValueTypeMax =
      std::numeric_limits<ValueType>::max();
  static constexpr const ValueType ValueTypeMin =
      std::numeric_limits<ValueType>::min();
  typedef size_t SizeType;

private:
  struct Datum {
  private:
    KeyType key;
    std::vector<ValueType> datum;
    bool deleted = false;

  public:
    Datum() = default;
    explicit Datum(const SizeType &size)
        : datum(std::vector<ValueType>(size, ValueType())) {}
    template <class ValueTypeContainer>
    explicit Datum(const KeyType &key, const ValueTypeContainer &datum) {
      this->key = key;
      this->datum = datum;
    }
    explicit Datum(const KeyType &key,
                   std::vector<ValueType> &&datum) noexcept {
      this->key = key;
      this->datum = std::move(datum);
    }
    bool operator==(const Datum & /*d*/) const { return this->key.empty(); }
    void delete_datum() { deleted = true; }
    void undelete_datum() { deleted = false; }
    bool datum_deleted() const { return deleted; }
    KeyType &getKey() { return key; }
    const KeyType &getKey() const { return key; }
    std::vector<ValueType> &getDatum() { return datum; }
    const std::vector<ValueType> &getDatum() const { return datum; }
  };

  typedef std::vector<Datum>::iterator DataIterator;
  typedef std::vector<Datum>::const_iterator ConstDataIterator;
  std::vector<FieldNameType> fields;
  std::unordered_map<FieldNameType, FieldIndex> fieldMap;
  std::vector<Datum> data;
  std::unordered_map<KeyType, SizeType> keyMap;
  std::string tableName;

public:
  typedef std::unique_ptr<Table> Ptr;

  template <class Iterator, class VType> class ObjectImpl {
    friend class Table;

    Iterator it;
    Table *table;

  public:
    typedef std::unique_ptr<ObjectImpl> Ptr;

    ObjectImpl(Iterator datumIt, const Table *t)
        : it(datumIt), table(const_cast<Table *>(t)) {}

    ObjectImpl(const ObjectImpl &) = default;
    ObjectImpl(ObjectImpl &&) noexcept = default;
    ObjectImpl &operator=(const ObjectImpl &) = default;
    ObjectImpl &operator=(ObjectImpl &&) noexcept = default;
    ~ObjectImpl() = default;
    KeyType key() const { return it->getKey(); }
    std::vector<ValueType> value() const { return it->getDatum(); }
    void remove() { it->delete_datum(); }
    void setKey(KeyType key) {
      auto keyMapIt = table->keyMap.find(it->getKey());
      auto dataIt = std::move(keyMapIt->second);
      table->keyMap.erase(keyMapIt);
      table->keyMap.emplace(key, std::move(dataIt));
      it->getKey() = std::move(key);
    }

    VType &operator[](const FieldNameType &field) const {
      _DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field);
    }
    VType &operator[](const FieldIndex &index) const {
      _DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index);
    }
    VType &get(const FieldNameType &field) const {
      _DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field);
    }
    VType &get(const FieldIndex &index) const {
      _DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index);
    }
  };

  typedef ObjectImpl<DataIterator, ValueType> Object;
  typedef ObjectImpl<ConstDataIterator, const ValueType> ConstObject;

  template <typename ObjType, typename DatumIterator> class IteratorImpl {
    using difference_type = std::ptrdiff_t;
    using value_type = ObjType;
    using pointer = typename ObjType::Ptr;
    using reference = ObjType;
    using iterator_category = std::random_access_iterator_tag;
    // See https://stackoverflow.com/questions/37031805/

    friend class Table;
    DatumIterator it;
    const Table *table = nullptr;

  public:
    IteratorImpl(DatumIterator datumIt, const Table *t)
        : it(datumIt), table(t) {}

    IteratorImpl() = default;
    IteratorImpl(const IteratorImpl &) = default;
    IteratorImpl(IteratorImpl &&) noexcept = default;
    IteratorImpl &operator=(const IteratorImpl &) = default;
    IteratorImpl &operator=(IteratorImpl &&) noexcept = default;
    ~IteratorImpl() = default;
    pointer operator->() { return createProxy(it, table); }
    reference operator*() { return *createProxy(it, table); }
    IteratorImpl operator+(int n) { return IteratorImpl(it + n, table); }
    IteratorImpl operator-(int n) { return IteratorImpl(it - n, table); }
    IteratorImpl &operator+=(int n) { return it += n, *this; }
    IteratorImpl &operator-=(int n) { return it -= n, *this; }
    IteratorImpl &operator++() {
      ++it;
      while (it != this->table->data.end() && it->datum_deleted())
        ++it;
      return *this;
    }
    IteratorImpl &operator--() { return --it, *this; }
    const IteratorImpl operator++(int) {
      auto retVal = IteratorImpl(*this);
      ++it;
      while (it != this->table->data.cend() && it->deleted)
        ++it;
      return retVal;
    }
    const IteratorImpl operator--(int) {
      auto retVal = IteratorImpl(*this);
      --it;
      return retVal;
    }

    bool operator==(const IteratorImpl &other) { return this->it == other.it; }
    bool operator!=(const IteratorImpl &other) { return this->it != other.it; }
    bool operator<=(const IteratorImpl &other) { return this->it <= other.it; }
    bool operator>=(const IteratorImpl &other) { return this->it >= other.it; }
    bool operator<(const IteratorImpl &other) { return this->it < other.it; }
    bool operator>(const IteratorImpl &other) { return this->it > other.it; }
    bool deleted() { return it->datum_deleted(); }
  };

  typedef IteratorImpl<Object, decltype(data.begin())> Iterator;
  typedef IteratorImpl<ConstObject, decltype(data.cbegin())> ConstIterator;

private:
  static ConstObject::Ptr createProxy(ConstDataIterator it,
                                      const Table *table) {
    return std::make_unique<ConstObject>(it, table);
  }

  static Object::Ptr createProxy(DataIterator it, const Table *table) {
    return std::make_unique<Object>(it, table);
  }

public:
  Table() = delete;
  explicit Table(std::string name) : tableName(std::move(name)) {}

  template <class FieldIDContainer>
  Table(const std::string &name, const FieldIDContainer &fields);

  Table(std::string name, const Table &origin)
      : fields(origin.fields), fieldMap(origin.fieldMap), data(origin.data),
        keyMap(origin.keyMap), tableName(std::move(name)) {}

  FieldIndex getFieldIndex(const FieldNameType &field) const;
  void insertByIndex(const KeyType &key, std::vector<ValueType> &&data);
  Object::Ptr operator[](const KeyType &key);
  void setName(std::string name) { this->tableName = std::move(name); }
  const std::string &name() const { return this->tableName; }
  bool empty() const { return this->data.empty(); }
  size_t size() const { return this->data.size(); }
  const std::vector<FieldNameType> &field() const { return this->fields; }
  size_t clear() {
    auto result = keyMap.size();
    data.clear();
    keyMap.clear();
    return result;
  }
  void erase(const std::vector<Table::KeyType> &keys) {
    for (const auto &key : keys) {
      (*this)[key]->remove();
    }
  }
  Iterator begin() {
    auto it = data.begin();
    while (it != data.end() && it->datum_deleted())
      it++;
    return {it, this};
  }
  Iterator origin_begin() { return {data.begin(), this}; }
  Iterator end() { return {data.end(), this}; }

  ConstIterator begin() const {
    auto it = data.cbegin();
    while (it != data.cend() && it->datum_deleted())
      it++;
    return {it, this};
  }
  ConstIterator origin_begin() const { return {data.cbegin(), this}; }
  ConstIterator end() const { return {data.cend(), this}; }

  bool duplicateEval(KeyType key) {
    key.append("_copy");
    auto it = this->keyMap.find(key);
    if (it != this->keyMap.end()) {
      DataIterator const datumIt =
          this->data.begin() +
          static_cast<std::vector<Table::Datum>::difference_type>(it->second);
      if (datumIt->datum_deleted())
        return false;
      return true;
    }
    return false;
  }
  friend std::ostream &operator<<(std::ostream &os, const Table &table);

private:
  FIFOSharedMutex table_mutex;

public:
  FIFOSharedMutex &get_table_mutex() { return table_mutex; }
};

std::ostream &operator<<(std::ostream &os, const Table &table);

template <class FieldIDContainer>
Table::Table(const std::string &name, const FieldIDContainer &fields)
    : fields(fields.cbegin(), fields.cend()), tableName(name) {
  SizeType i = 0;
  for (const auto &f : fields) {
    if (f == "KEY")
      throw MultipleKey("Error creating table \"" + name +
                        "\": Multiple KEY field.");
    fieldMap.emplace(f, i++);
  }
}

#endif // PROJECT_DB_TABLE_H
