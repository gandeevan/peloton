//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_plan.cpp
//
// Identification: src/planner/alter_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/alter_plan.h"

#include "parser/alter_statement.h"
#include "storage/data_table.h"
#include "catalog/catalog.h"

namespace peloton {
namespace planner {

AlterPlan::AlterPlan(storage::DataTable *table) {
  target_table_ = table;
  missing = false;
}

/*DropPlan::DropPlan(std::string name) {
  table_name = name;
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, table_name);
  missing = false;
}*/

AlterPlan::AlterPlan(parser::AlterStatement *parse_tree) {
  table_name = parse_tree->GetTableName();
  // Set it up for the moment , cannot seem to find it in DropStatement
//  missing = parse_tree->missing;

  try {
    target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
        parse_tree->GetDatabaseName(), table_name);
  }
  catch (CatalogException &e) {
    // Dropping a table which doesn't exist
    //if (missing == false) {
      throw e;
    //}
  }
}

}  // namespace planner
}  // namespace peloton
