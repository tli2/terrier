#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"

namespace terrier {

namespace binder {
class BinderContext;
}

namespace parser {

/**
 * ColumnValueExpression represents a reference to a column.
 */
class ColumnValueExpression : public AbstractExpression {
 public:
  /**
   * This constructor is called only in postgresparser, setting the column name,
   * and optionally setting the table name and alias.
   * Namespace name is always set to empty string, as the postgresparser does not know the namespace name.
   * Parameter namespace name is included so that the program can differentiate this constructor from
   * another constructor that sets the namespace name, table name. and column name.
   * @param namespace_name namespace name
   * @param table_name table name
   * @param col_name column name
   * @param alias alias of the expression
   */
  ColumnValueExpression(std::string namespace_name, std::string table_name, std::string col_name, std::string alias)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type::TypeId::INVALID, std::move(alias), {}),
        namespace_name_(std::move(namespace_name)),
        table_name_(std::move(table_name)),
        column_name_(std::move(col_name)) {}

  /**
   * @param table_name table name
   * @param col_name column name
   */
  ColumnValueExpression(std::string table_name, std::string col_name)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type::TypeId::INVALID, {}),
        table_name_(std::move(table_name)),
        column_name_(std::move(col_name)) {}

  /**
   * @param namespace_name namespace name
   * @param table_name table name
   * @param col_name column name
   */
  ColumnValueExpression(std::string namespace_name, std::string table_name, std::string col_name)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type::TypeId::INVALID, {}),
        namespace_name_(std::move(namespace_name)),
        table_name_(std::move(table_name)),
        column_name_(std::move(col_name)) {}

  /**
   * @param database_oid database OID
   * @param table_oid table OID
   * @param column_oid column OID
   */
  ColumnValueExpression(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, catalog::col_oid_t column_oid)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type::TypeId::INVALID, {}),
        database_oid_(database_oid),
        table_oid_(table_oid),
        column_oid_(column_oid) {}

  /** Default constructor for deserialization. */
  ColumnValueExpression() = default;

  /** @return namespace name */
  std::string GetNamespaceName() const { return namespace_name_; }

  /** @return table name */
  std::string GetTableName() const { return table_name_; }

  /** @return column name */
  std::string GetColumnName() const { return column_name_; }

  /** @return database oid */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /** @return table oid */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /** @return column oid */
  catalog::col_oid_t GetColumnOid() const { return column_oid_; }

  std::unique_ptr<AbstractExpression> Copy() const override {
    auto expr = std::make_unique<ColumnValueExpression>(GetDatabaseOid(), GetTableOid(), GetColumnOid());
    expr->SetMutableStateForCopy(*this);
    expr->namespace_name_ = this->namespace_name_;
    expr->table_name_ = this->table_name_;
    expr->column_name_ = this->column_name_;
    expr->SetDatabaseOID(this->database_oid_);
    expr->SetTableOID(this->table_oid_);
    expr->SetColumnOID(this->column_oid_);
    return expr;
  }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_oid_));
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const ColumnValueExpression &>(rhs);
    if (GetColumnName() != other.GetColumnName()) return false;
    if (GetTableName() != other.GetTableName()) return false;
    if (GetNamespaceName() != other.GetNamespaceName()) return false;
    if (GetColumnOid() != other.GetColumnOid()) return false;
    if (GetTableOid() != other.GetTableOid()) return false;
    return GetDatabaseOid() == other.GetDatabaseOid();
  }

  void DeriveExpressionName() override {
    if (!(this->GetAlias().empty()))
      this->SetExpressionName(this->GetAlias());
    else
      this->SetExpressionName(column_name_);
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["namespace_name"] = namespace_name_;
    j["table_name"] = table_name_;
    j["column_name"] = column_name_;
    j["database_oid"] = database_oid_;
    j["table_oid"] = table_oid_;
    j["column_oid"] = column_oid_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    namespace_name_ = j.at("namespace_name").get<std::string>();
    table_name_ = j.at("table_name").get<std::string>();
    column_name_ = j.at("column_name").get<std::string>();
    database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
    table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
    column_oid_ = j.at("column_oid").get<catalog::col_oid_t>();
    return exprs;
  }

 private:
  friend class binder::BinderContext;
  /** @param database_oid Database OID to be assigned to this expression */
  void SetDatabaseOID(catalog::db_oid_t database_oid) { database_oid_ = database_oid; }
  /** @param table_oid Table OID to be assigned to this expression */
  void SetTableOID(catalog::table_oid_t table_oid) { table_oid_ = table_oid; }
  /** @param column_oid Column OID to be assigned to this expression */
  void SetColumnOID(catalog::col_oid_t column_oid) { column_oid_ = column_oid; }
  /** @param table_oid Table OID to be assigned to this expression */
  void SetTableName(const std::string &table_name) { table_name_ = std::string(table_name); }
  /** @param column_oid Column OID to be assigned to this expression */
  void SetColumnName(const std::string &col_name) { column_name_ = std::string(col_name); }

  /** Namespace name. */
  std::string namespace_name_;
  /** Table name. */
  std::string table_name_;
  /** Column name. */
  std::string column_name_;

  /** OID of the database */
  catalog::db_oid_t database_oid_ = catalog::INVALID_DATABASE_OID;

  /** OID of the table */
  catalog::table_oid_t table_oid_ = catalog::INVALID_TABLE_OID;

  /** OID of the column */
  catalog::col_oid_t column_oid_ = catalog::INVALID_COLUMN_OID;
};

DEFINE_JSON_DECLARATIONS(ColumnValueExpression);

}  // namespace parser
}  // namespace terrier