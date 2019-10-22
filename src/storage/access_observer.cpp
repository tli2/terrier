#include "storage/access_observer.h"
#include "storage/block_compactor.h"
#include "storage/dirty_globals.h"
#include "tpcc/database.h"

namespace terrier::storage {
void AccessObserver::ObserveGCInvocation() {
  gc_epoch_++;
  for (auto it = last_touched_.begin(), end = last_touched_.end(); it != end;) {
    if (it->second + COLD_DATA_EPOCH_THRESHOLD < gc_epoch_) {
      auto hash = reinterpret_cast<uintptr_t>(it->first) >> 4;
      if (hash % 2 == 0)
        compactor_1->PutInQueue(it->first);
      else
        compactor_2->PutInQueue(it->first);
      it = last_touched_.erase(it);
    } else {
      ++it;
    }
  }
}

void AccessObserver::ObserveWrite(RawBlock *block) {
  if (block->insert_head_ == block->data_table_->accessor_.GetBlockLayout().NumSlots()
      && DirtyGlobals::tpcc_db->ShouldTransform(block->data_table_))
    last_touched_[block] = gc_epoch_;
}

}  // namespace terrier::storage
