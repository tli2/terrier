#include "storage/block_compactor.h"
#include <algorithm>
#include <unordered_map>
#include <utility>
#include <vector>
namespace terrier::storage {
namespace {
// for empty callback
void NoOp(void * /* unused */) {}
}  // namespace

void BlockCompactor::ProcessCompactionQueue(transaction::TransactionManager *txn_manager) {
  std::forward_list<std::pair<RawBlock *, DataTable *>> to_process = std::move(compaction_queue_);

  for (auto &entry : to_process) {
    BlockAccessController &controller = entry.first->controller_;
    if (controller.GetBlockState()->load() == BlockAccessController::BlockState::COOLING) {
        if (!CheckForVersionsAndGaps(entry.second->accessor_, entry.first)) continue;
        GatherVarlens(entry.first, entry.second);
        controller.GetBlockState()->store(BlockAccessController::BlockState::FROZEN);
    } else {
      // TODO(Tianyu): This is probably fine for now, but we will want to not only compact within a block
      // but also across blocks to eventually free up slots
      CompactionGroup cg(txn_manager->BeginTransaction(), entry.second);
      cg.blocks_to_compact_.emplace(entry.first, std::vector<uint32_t>());

      // Block can still be inserted into. Hands off.
      if (entry.first->insert_head_ != entry.second->accessor_.GetBlockLayout().NumSlots()) continue;
      ScanForGaps(&cg);
      if (EliminateGaps(&cg)) {
        txn_manager->Commit(cg.txn_, NoOp, nullptr);
        BlockAccessController &controller = entry.first->controller_;
        controller.GetBlockState()->store(BlockAccessController::BlockState::COOLING);
      } else {
        txn_manager->Abort(cg.txn_);
      }
    }
  }
}

void BlockCompactor::ScanForGaps(CompactionGroup *cg) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  for (auto &entry : cg->blocks_to_compact_) {
    RawBlock *block = entry.first;
    std::vector<uint32_t> &empty_slots = entry.second;
    TERRIER_ASSERT(block->insert_head_ == layout.NumSlots(), "The block should be full to stop inserts from coming in");

    // We will loop through each block and figure out if we are safe to proceed with compaction and identify
    // any gaps
    auto *bitmap = accessor.AllocationBitmap(block);
    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++)
      if (!bitmap->Test(offset)) empty_slots.push_back(offset);
  }
}

bool BlockCompactor::EliminateGaps(CompactionGroup *cg) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // TODO(Tianyu): This process can probably be optimized further for the least amount of movements of tuples. But we
  // are probably close enough to optimal that it does not matter that much
  // Within a group, we can calculate the number of blocks exactly we need to store all the filled tuples (we may
  // or may not need to throw away extra blocks when we are done compacting). Then, the algorithm involves selecting
  // the blocks with the least number of empty slots as blocks to "fill into", and the rest as blocks to "take away
  // from". These are not two disjoint sets as we will probably need to shuffle tuples within one block to have
  // perfectly compact groups (but only one block within a group needs this)
  std::vector<RawBlock *> all_blocks;
  for (auto &entry : cg->blocks_to_compact_)
    all_blocks.push_back(entry.first);

  // Sort all the blocks within a group based on the number of filled slots, in descending order.
  std::sort(all_blocks.begin(), all_blocks.end(), [&](RawBlock *a, RawBlock *b) {
    auto a_empty = cg->blocks_to_compact_[a].size();
    auto b_empty = cg->blocks_to_compact_[b].size();
    // We know these finds will not return end() because we constructed the vector from the map
    return a_empty < b_empty;
  });

  // We assume that there are a lot more filled slots than empty slots, so we only store the list of empty slots
  // and construct the vector of filled slots on the fly in order to reduce the memory footprint.
  std::vector<uint32_t> filled;
  // Because we constructed the two lists from sequential scan, slots will always appear in order. We
  // essentially will fill gaps in order, by using the real tuples in reverse order. (Take the last tuple to
  // fill the first empty slot)
  for (auto taker = all_blocks.begin(), giver = all_blocks.end(); taker <= giver; taker++) {
    // Again, we know these finds will not return end() because we constructed the vector from the map
    std::vector<uint32_t> &taker_empty = cg->blocks_to_compact_.find(*taker)->second;

    for (uint32_t empty_offset : taker_empty) {
      if (filled.empty()) {
        giver--;
        ComputeFilled(layout, &filled, cg->blocks_to_compact_.find(*giver)->second);
      }
      TupleSlot empty_slot(*taker, empty_offset);
      // fill the first empty slot with the last filled slot, essentially
      // We will only shuffle tuples within a block if it is the last block to compact. Then, we can stop
      TupleSlot filled_slot(*giver, filled.back());
      filled.pop_back();
      // when the next empty slot is logically after the next filled slot (which implies we are processing
      // an empty slot that would be empty in a compact block)
      if (taker == giver && filled_slot.GetOffset() < empty_slot.GetOffset()) break;
      // A failed move implies conflict
      if (!MoveTuple(cg, filled_slot, empty_slot)) return false;
    }
  }

  // TODO(Tianyu): This compaction process could leave blocks empty within a group and we will need to figure out
  // how those blocks are garbage collected. These blocks should have the same life-cycle as the compacting
  // transaction itself. (i.e. when the txn context is being GCed, we should be able to free these blocks as well)
  // For now we are not implementing this because each compaction group is one block.

  return true;
}

bool BlockCompactor::MoveTuple(CompactionGroup *cg, TupleSlot from, TupleSlot to) {
  const TupleAccessStrategy &accessor = cg->table_->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();

  // Read out the tuple to copy
  RedoRecord *record = cg->txn_->StageWrite(cg->table_, to, cg->all_cols_initializer_);
  bool valid UNUSED_ATTRIBUTE = cg->table_->Select(cg->txn_, from, record->Delta());
  TERRIER_ASSERT(valid, "this read should not return an invisible tuple");
  // Because the GC will assume all varlen pointers are unique and deallocate the same underlying
  // varlen for every update record, we need to mark subsequent records that reference the same
  // varlen value as not reclaimable so as to not double-free
  for (col_id_t varlen_col_id : layout.Varlens()) {
    // We know this to be true because the projection list has all columns
    auto offset = static_cast<uint16_t>(!varlen_col_id - NUM_RESERVED_COLUMNS);
    auto *entry = reinterpret_cast<VarlenEntry *>(record->Delta()->AccessWithNullCheck(offset));
    if (entry == nullptr) continue;
    if (entry->Size() <= VarlenEntry::InlineThreshold()) {
      *entry = VarlenEntry::CreateInline(entry->Content(), entry->Size());
    } else {
      // TODO(Tianyu): Copying for correctness. This can potentially be expensive
      byte *copied = common::AllocationUtil::AllocateAligned(entry->Size());
      std::memcpy(copied, entry->Content(), entry->Size());
      *entry = VarlenEntry::Create(copied, entry->Size(), true);
    }
  }

  // Copy the tuple into the empty slot
  // This operation cannot fail since a logically deleted slot can only be reclaimed by the compaction thread
  accessor.Reallocate(to);
  cg->table_->InsertInto(cg->txn_, *record->Delta(), to);

  // The delete can fail if a concurrent transaction is updating said tuple. We will have to abort if this is
  // the case.
  return cg->table_->Delete(cg->txn_, from);
}

bool BlockCompactor::CheckForVersionsAndGaps(const TupleAccessStrategy &accessor, RawBlock *block) {
  const BlockLayout &layout = accessor.GetBlockLayout();


  auto *allocation_bitmap = accessor.AllocationBitmap(block);
  auto *version_ptrs = reinterpret_cast<UndoRecord **>(accessor.ColumnStart(block, VERSION_POINTER_COLUMN_ID));
  // We will loop through each block and figure out if any versions showed up between our current read and the
  // earlier read
  for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
    // TODO(Tianyu): Test contiguous.
    if (!allocation_bitmap->Test(offset)) continue;
    auto *record = version_ptrs[offset];
    // Any version chain should be from our compaction transaction.
    if (record != nullptr) return false;
  }
  auto state = BlockAccessController::BlockState::COOLING;
  // Check that no other transaction has modified the canary in the block header. If we fail it's okay
  // to leave the block header because someone else must have already flipped it to hot
  return block->controller_.GetBlockState()->compare_exchange_strong(state,
                                                                     BlockAccessController::BlockState::FREEZING);
}

void BlockCompactor::GatherVarlens(RawBlock *block, DataTable *table) {
  const TupleAccessStrategy &accessor = table->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  ArrowBlockMetadata &metadata = accessor.GetArrowBlockMetadata(block);
  auto *allocation_bitmap = accessor.AllocationBitmap(block);
  for (uint i = 0; i < layout.NumSlots(); i++) {
    // At the end of allocated block, since the block is guaranteed to be compact at this point
    if (!allocation_bitmap->Test(i)) {
      metadata.NumRecords() = i;
      break;
    }
  }

  std::vector<const byte *> ptrs;
  for (col_id_t col_id : layout.AllColumns()) {
    common::RawConcurrentBitmap *column_bitmap = accessor.ColumnNullBitmap(block, col_id);
    if (!layout.IsVarlen(col_id)) {
      // Only need to count null for non-varlens
      for (uint32_t i = 0; i < metadata.NumRecords(); i++)
        if (!column_bitmap->Test(i)) metadata.NullCount(col_id)++;
    } else {
      ArrowVarlenColumn &col = metadata.GetColumnInfo(layout, col_id).varlen_column_;
      auto *values = reinterpret_cast<VarlenEntry *>(accessor.ColumnStart(block, col_id));

      // Read through every tuple
      for (uint32_t i = 0; i < metadata.NumRecords(); i++) {
        if (!column_bitmap->Test(i))
          // Update null count
          metadata.NullCount(col_id)++;
        else
          // count the total size of varlens
          col.values_length_ += values[i].Size();
      }

      col.offsets_length_ = metadata.NumRecords() + 1;
      col.Allocate();
      for (uint32_t i = 0, acc = 0; i < metadata.NumRecords(); i++) {
        if (!column_bitmap->Test(i)) continue;
        // Only do a gather operation if the column is varlen
        VarlenEntry &entry = values[i];
        std::memcpy(col.values_ + acc, entry.Content(), entry.Size());
        col.offsets_[i] = acc;

        if (entry.NeedReclaim())
          ptrs.push_back(entry.Content());
        // TODO(Tianyu): Describe why this is still safe
        if (entry.Size() > VarlenEntry::InlineThreshold())
          entry = VarlenEntry::Create(col.values_ + acc, entry.Size(), false);
        acc += entry.Size();
      }
      col.offsets_[metadata.NumRecords()] = col.values_length_;
    }
  }
  (void) ptrs;
}

}  // namespace terrier::storage
