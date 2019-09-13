#pragma once
#include <utility>
#include <vector>
#include "common/container/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/arrow_block_metadata.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "tuple_access_strategy.h"

namespace terrier::storage {
class DataTable;

class RowAccessStrategy {
 public:
  explicit RowAccessStrategy(const std::vector<uint32_t> &attr_sizes)
      : attr_sizes_(attr_sizes), layout_(ComputeLayout()) {

  }

  void InitializeRawBlock(storage::DataTable *data_table, RawBlock *raw, layout_version_t layout_version) const {
    underlying_.InitializeRawBlock(data_table, raw, layout_version);
  }

  bool Allocated(const TupleSlot slot) const {
    return underlying_.Allocated(slot);
  }

  /**
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return a pointer to the attribute, or nullptr if attribute is null.
   */
  byte *AccessWithNullCheck(const TupleSlot slot, const col_id_t col_id) const {
    if (col_id == col_id_t(0)) return underlying_.AccessWithNullCheck(slot, col_id);
    byte *tuple_start = underlying_.AccessWithoutNullCheck(slot, col_id_t(1));
    if (!reinterpret_cast<common::RawBitmap *>(tuple_start)->Test(!col_id)) return nullptr;
    return tuple_start + attr_offsets_[!col_id];
  }

  /**
   * Returns a pointer to the attribute, ignoring the presence bit.
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return a pointer to the attribute
   */
  byte *AccessWithoutNullCheck(const TupleSlot slot, const col_id_t col_id) const {
    if (col_id == col_id_t(0)) return underlying_.AccessWithoutNullCheck(slot, col_id);
    byte *tuple_start = underlying_.AccessWithoutNullCheck(slot, col_id_t(1));
    return tuple_start + attr_offsets_[!col_id];
  }

  /**
   * Returns a pointer to the attribute. If the attribute is null, set null to
   * false.
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return a pointer to the attribute.
   */
  byte *AccessForceNotNull(const TupleSlot slot, const col_id_t col_id) const {
    if (col_id == col_id_t(0)) return underlying_.AccessForceNotNull(slot, col_id);
    byte *tuple_start = underlying_.AccessWithoutNullCheck(slot, col_id_t(1));
    auto *bitmap = reinterpret_cast<common::RawBitmap *>(tuple_start);
    if (!bitmap->Test(!col_id)) bitmap->Flip(!col_id);
    return tuple_start + attr_offsets_[!col_id];
  }

  /**
   * Get an attribute's null value
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return true if null, false otherwise
   */
  bool IsNull(const TupleSlot slot, const col_id_t col_id) const {
    if (col_id == col_id_t(0)) return underlying_.IsNull(slot, col_id);
    byte *tuple_start = underlying_.AccessWithoutNullCheck(slot, col_id_t(1));
    return !reinterpret_cast<common::RawBitmap *>(tuple_start)->Test(!col_id);
  }

  /**
   * Set an attribute null.
   * @param slot tuple slot to access
   * @param col_id id of the column
   */
  void SetNull(const TupleSlot slot, const col_id_t col_id) const {
    if (col_id == col_id_t(0)) {
      underlying_.SetNull(slot, col_id);
      return;
    }
    byte *tuple_start = underlying_.AccessWithoutNullCheck(slot, col_id_t(1));
    reinterpret_cast<common::RawBitmap *>(tuple_start)->Set(!col_id, false);
  }

  /**
   * Set an attribute not null.
   * @param slot tuple slot to access
   * @param col_id id of the column
   */
  void SetNotNull(const TupleSlot slot, const col_id_t col_id) const {
    if (col_id == col_id_t(0)) {
      underlying_.SetNotNull(slot, col_id);
      return;
    }
    byte *tuple_start = underlying_.AccessWithoutNullCheck(slot, col_id_t(1));
    reinterpret_cast<common::RawBitmap *>(tuple_start)->Set(!col_id, true);
  }

  /**
   * Flip a deallocated slot to be allocated again. This is useful when compacting a block,
   * as we want to make decisions in the compactor on what slot to use, not in this class.
   * This method should not be called other than that.
   * @param slot the tuple slot to reallocate. Must be currently deallocated.
   */
  void Reallocate(TupleSlot slot) const {
    return underlying_.Reallocate(slot);
  }

  /**
   * Allocates a slot for a new tuple, writing to the given reference.
   * @param block block to allocate a slot in.
   * @param[out] slot tuple to write to.
   * @return true if the allocation succeeded, false if no space could be found.
   */
  bool Allocate(RawBlock *block, TupleSlot *slot) const {
    return underlying_.Allocate(block, slot);
  }

  /**
   * @param block the block to access
   * @return pointer to the allocation bitmap of the block
   */
  common::RawConcurrentBitmap *AllocationBitmap(RawBlock *block) const {
    return underlying_.AllocationBitmap(block);
  }

  /**
   * Deallocates a slot.
   * @param slot the slot to free up
   */
  void Deallocate(const TupleSlot slot) const {
    underlying_.Deallocate(slot);
  }

  /**
   * Returns the block layout.
   * @return the block layout.
   */
  const BlockLayout &GetBlockLayout() const { return layout_; }

  const std::vector<uint32_t> &AttrSizes() const { return attr_sizes_; }

 private:
  std::vector<uint32_t> attr_sizes_;
  std::vector<uint32_t> attr_offsets_;
  const BlockLayout layout_;
  const TupleAccessStrategy underlying_{layout_};
  // Start of each mini block, in offset to the start of the block


  BlockLayout ComputeLayout() {
    std::sort(attr_sizes_.begin() + NUM_RESERVED_COLUMNS, attr_sizes_.end(), std::greater<>());
    attr_offsets_.push_back(0);
    uint32_t tuple_size = common::RawBitmap::SizeInBytes(attr_sizes_.size() - 1);
    tuple_size = StorageUtil::PadUpToSize(8, tuple_size);

    for (uint32_t i = 1; i < static_cast<uint32_t>(attr_sizes_.size()); i++) {
      attr_offsets_.push_back(tuple_size);
      tuple_size += attr_sizes_[i];
    }

    tuple_size = StorageUtil::PadUpToSize(8, tuple_size);

    return BlockLayout({8, tuple_size});
  }
};} // namespace terrier::storage