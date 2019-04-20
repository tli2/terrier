#include "storage/access_observer.h"
#include "storage/block_compactor.h"
#include "storage/dirty_globals.h"
#include "tpcc/database.h"

namespace terrier::storage {
void AccessObserver::ObserveGCInvocation() {
  gc_epoch_++;
  for (auto it = last_touched_.begin(), end = last_touched_.end(); it != end;) {
    if (it->second.first + COLD_DATA_EPOCH_THRESHOLD < gc_epoch_) {
      compactor_->PutInQueue({it->first, it->second.second});
      it = last_touched_.erase(it);
    } else {
      ++it;
    }
  }
}

void AccessObserver::ObserveWrite(DataTable *table, RawBlock *block) {
  if (block->insert_head_ == table->accessor_.GetBlockLayout().NumSlots()
      && DirtyGlobals::tpcc_db->ShouldTransform(table))
    last_touched_[block] = {gc_epoch_, table};
}

}  // namespace terrier::storage
