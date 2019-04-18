#pragma once

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "storage/storage_defs.h"

namespace terrier::storage {
class DataTable;
class BlockCompactor;
// TODO(Tianyu): Probably need to be smarter than this to identify true hot or cold data, but this
// will do for now.
#define COLD_DATA_EPOCH_THRESHOLD 10
/**
 * The access observer is attached to the storage engine's garbage collector in order to make decisions about
 * whether a block is cooling down from frequent access. Its observe methods are invoked from the garbage collector
 * when relavent events fire. It is then free to make a decision whether to send a block into the compactor's queue
 * to freeze asynchronously.
 *
 * Notice that although the observation step is light weight, it does happen on the garbage collection thread and thus
 * has some minor performance impact on GC and consequently the rest of the system. Care should be taken to not do
 * any computationally-intensive work here to figure out whether a block is cold. The entire hot-cold mechanism is
 * designed to be lightweight on the cold->hot transition so we can afford to be wrong in the observation phase.
 */
class AccessObserver {
 public:
  /**
   * Constructs a new AccessObserver that will send its observations to the given block compactor
   * @param compactor the compactor to use after identifying a cold block
   */
  explicit AccessObserver(BlockCompactor *compactor) : compactor_(compactor) {}

  /**
   * Signals to the AccessObserver that a new GC run has begun. This is useful as a measurement of time to the
   * AccessObserver as it uses the number of GC invocations as an approximate clock.
   */
  void ObserveGCInvocation();
  /**
   * Observe a write to the given tuple slot from the given data table.
   *
   * Notice that not all writes will be captured in this case. For example, an aborted transaction might not show up
   * here. All committed transactions are guaranteed to show up here.
   */
  void ObserveWrite(DataTable *table, RawBlock *slot);

 private:
  uint64_t gc_epoch_ = 0;  // estimate time using the number of times GC has run
  // TODO(Tianyu): This is hardly a space efficient representation of blocks. However this should do
  // since we assume that only a small portion of the database will be hot
  // Here RawBlock * should suffice as a unique identifier of the block. Although a block can be
  // reused, that process should only be triggered through compaction, which happens only if the
  // reference to said block is identified as cold and leaves the table.
  std::unordered_map<RawBlock *, std::pair<uint64_t, DataTable *>> last_touched_;
  // Sorted table references by epoch. We can easily do range scans on this data structure to get
  // cold blocks given current epoch and some threshold for a block to be cold.
//  std::map<uint64_t, std::unordered_map<RawBlock *, DataTable *>> table_references_by_epoch_;
//  std::unordered_set<RawBlock *> no_longer_insertable_;
  BlockCompactor *const compactor_;
};
}  // namespace terrier::storage
