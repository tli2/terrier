#include "storage/access_observer.h"
#include "storage/block_compactor.h"

namespace terrier::storage {
void AccessObserver::ObserveGCInvocation() {
  gc_epoch_++;
  for (auto &entry : last_touched_) {
    if (entry.second.first + COLD_DATA_EPOCH_THRESHOLD < gc_epoch_)
      compactor_->PutInQueue({entry.first, entry.second.second});
  }
}

void AccessObserver::ObserveWrite(DataTable *table, TupleSlot slot) {
  RawBlock *block = slot.GetBlock();
  if (block->insert_head_ == table->accessor_.GetBlockLayout().NumSlots())
    last_touched_[block] = {gc_epoch_, table};
}

}  // namespace terrier::storage
