#pragma once

#include <cstdint>
#include <string>
#include "common/hash_util.h"
#include "common/typedefs.h"
#include "type/type_id.h"

namespace terrier::type {

class Value {
 public:
  ~Value();
  // copy constructor
  Value(const Value &other);

  // scalar constructors
  explicit Value(boolean_t value);
  explicit Value(int8_t value);
  explicit Value(int16_t value);
  explicit Value(int32_t value);
  explicit Value(int64_t value);
  explicit Value(double value);
  explicit Value(timestamp_t value);
  explicit Value(date_t value);

  // varchar
  explicit Value(const std::string &value);
  // varbinary
  Value(const char *data, uint32_t len);

  TypeId GetType() const { return type_id_; }

  // value retrieval methods
  const boolean_t *GetBooleanValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::BOOLEAN, "The type must be a boolean");
    return &value_.boolean;
  }

  const int8_t *GetTinyIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::TINYINT, "The type must be a tinyint");
    return &value_.tinyint;
  }

  const int16_t *GetSmallIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::SMALLINT, "The type must be a smallint");
    return &value_.smallint;
  }

  const int32_t *GetIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::INTEGER, "The type must be a integer");
    return &value_.integer;
  }

  const int64_t *GetBigIntValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::BIGINT, "The type must be a bigint");
    return &value_.bigint;
  }

  const double *GetDecimalValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::DECIMAL, "The type must be a decimal");
    return &value_.decimal;
  }

  const timestamp_t *GetTimestampValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::TIMESTAMP, "The type must be a timestamp");
    return &value_.timestamp;
  }

  const date_t *GetDateValue() const {
    TERRIER_ASSERT(type_id_ == TypeId::DATE, "The type must be a date");
    return &value_.date;
  }

  bool operator==(const Value &rhs) const;

  hash_t Hash() const {
    hash_t ret_hash = 0;

    switch (type_id_) {
      case TypeId::BOOLEAN:
        ret_hash = HashUtil::Hash(GetBooleanValue());
        break;

      case TypeId::TINYINT:
        ret_hash = HashUtil::Hash(GetTinyIntValue());
        break;

      case TypeId::SMALLINT:
        ret_hash = HashUtil::Hash(GetSmallIntValue());
        break;

      case TypeId::INTEGER:
        ret_hash = HashUtil::Hash(GetIntValue());
        break;

      case TypeId::BIGINT:
        ret_hash = HashUtil::Hash(GetBigIntValue());
        break;

      case TypeId::DATE:
        ret_hash = HashUtil::Hash(GetDateValue());
        break;

      case TypeId::DECIMAL:
        ret_hash = HashUtil::Hash(GetDecimalValue());
        break;

      case TypeId::TIMESTAMP:
        ret_hash = HashUtil::Hash(GetTimestampValue());
        break;

      default:
        TERRIER_ASSERT(false, "unsupported type");
        break;
    }
    return ret_hash;
  }

 protected:
  union Val {
    boolean_t boolean;
    int8_t tinyint;
    int16_t smallint;
    int32_t integer;
    int64_t bigint;
    double decimal;
    timestamp_t timestamp;
    date_t date;
    char *varchar;
    // const char *const_varlen;
    // char *array;
  } value_;

  // For variable length types
  uint32_t var_len;

  // type of this Value
  TypeId type_id_;
};

}  // namespace terrier::type
