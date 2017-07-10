//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_create.h
//
// Identification: src/include/parser/alter_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/types.h"
#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "expression/abstract_expression.h"
#include "parser/create_statement.h"

namespace peloton {
namespace parser {



/**
 * @struct AlterStatement
 * @brief Represents "ALTER TABLE students ADD (name TEXT, student_number INTEGER,
 * city TEXT, grade DOUBLE)"
 */
struct AlterStatement : TableRefStatement {
  enum AlterType { kAddColumn, kDropColumn };

  AlterStatement(AlterType type)
      : TableRefStatement(StatementType::ALTER),
        type(type),
        if_not_exists(false),
        columns(nullptr){}

  virtual ~AlterStatement() {
    if (columns != nullptr) {
      for (auto col : *columns) delete col;
      delete columns;
    }

    if (database_name != nullptr) {
      delete[] (database_name);
    }
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  AlterType type;
  bool if_not_exists;

  std::vector<ColumnDefinition*>* columns;
  char* database_name = nullptr;
  bool unique = false;
};

}  // End parser namespace
}  // End peloton namespace
