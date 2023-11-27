//
// Created by liu on 18-10-23.
//

#include "Table.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include "Database.h"

Table::FieldIndex
Table::getFieldIndex(const Table::FieldNameType &field) const {
  try {
    return this->fieldMap.at(field);
  } catch (const std::out_of_range &e) {
    throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f %
                             (field.c_str()));
  }
}

void Table::insertByIndex(const KeyType &key, std::vector<ValueType> &&data) {
  auto it = this->keyMap.find(key);
  if (it != this->keyMap.end()) {
    DataIterator const datumIt =
        this->data.begin() +
        static_cast<std::vector<Table::Datum>::difference_type>(it->second);
    if (datumIt->datum_deleted()) {
      datumIt->undelete_datum();
      datumIt->getDatum() = data;
    } else {
      std::string const err = "In Table \"" + this->tableName + "\" : Key \"" +
                              key + "\" already exists!";
      throw ConflictingKey(err);
    }
  } else {
    this->keyMap.emplace(key, this->data.size());
    this->data.emplace_back(key, data);
  }
}

Table::Object::Ptr Table::operator[](const Table::KeyType &key) {
  auto it = keyMap.find(key);
  if (it == keyMap.end()) {
    // not found
    return nullptr;
  } else {
    DataIterator const datumIt =
        data.begin() +
        static_cast<std::vector<Table::Datum>::difference_type>(it->second);
    if (datumIt->datum_deleted())
      return nullptr;
    else
      return createProxy(datumIt, this);
  }
}

std::ostream &operator<<(std::ostream &os, const Table &table) {
  const int width = 10;
  std::stringstream buffer;
  buffer << table.tableName << "\t" << (table.fields.size() + 1) << "\n";
  buffer << std::setw(width) << "KEY";
  for (const auto &field : table.fields) {
    buffer << std::setw(width) << field;
  }
  buffer << "\n";
  auto numFields = table.fields.size();
  for (const auto &datum : table.data) {
    if (datum.datum_deleted())
      continue;
    buffer << std::setw(width) << datum.getKey();
    for (decltype(numFields) i = 0; i < numFields; ++i) {
      buffer << std::setw(width) << (datum.getDatum())[i];
    }
    buffer << "\n";
  }
  return os << buffer.str();
}
