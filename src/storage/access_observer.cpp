#include "storage/access_observer.h"
#include "storage/block_compactor.h"

namespace terrier::storage {
void AccessObserver::ObserveGCInvocation() {
  gc_epoch_++;
  auto it = table_references_by_epoch_.begin();
  for (auto end = table_references_by_epoch_.end();
       it != end; ++it) {
    auto &entry = *it;
    // Data still within threshold
    if (entry.first + COLD_DATA_EPOCH_THRESHOLD >= gc_epoch_) break;
    // Otherwise, we consider the block cold and can shove them into the compactor's queue for
    // processing
    for (auto &pair : entry.second)
      compactor_->PutInQueue(pair);
  }
  table_references_by_epoch_.erase(table_references_by_epoch_.begin(), it);
}

void AccessObserver::ObserveWrite(DataTable *table, TupleSlot slot) {
  RawBlock *block = slot.GetBlock();
  // No-op if this is already referenced this epoch
  table_references_by_epoch_[gc_epoch_].emplace(block, table);
  auto last_referenced_epoch = last_touched_.find(block);
  if (slot.GetOffset() == table->accessor_.GetBlockLayout().NumSlots()) no_longer_insertable_.insert(slot.GetBlock());
  // Do not remove if the block is accessed twice in a single epoch
  if (last_referenced_epoch != last_touched_.end() && last_referenced_epoch->second != gc_epoch_) {
    // Remove reference to last access's epoch as it is no longer the most recent access
    table_references_by_epoch_[last_referenced_epoch->second].erase(block);
    last_referenced_epoch->second = gc_epoch_;
  } else {
    last_touched_.emplace(block, gc_epoch_);
  }
}

}  // namespace terrier::storage
