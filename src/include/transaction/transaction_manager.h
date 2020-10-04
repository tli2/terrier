#pragma once
#include <unordered_set>
#include <utility>
#include "common/shared_latch.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/undo_record.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
/**
 * A transaction manager maintains global state about all running transactions, and is responsible for creating,
 * committing and aborting transactions
 */
class TransactionManager {
  // TODO(Tianyu): Implement the global transaction tables
 public:
  /**
   * Initializes a new transaction manager. Transactions will use the given object pool as source of their undo
   * buffers.
   * @param buffer_pool the buffer pool to use for transaction undo buffers
   * @param gc_enabled true if txns should be stored in a local queue to hand off to the GC, false otherwise
   * @param log_manager the log manager in the system, or LOGGING_DISABLED(nulllptr) if logging is turned off.
   */
  TransactionManager(storage::RecordBufferSegmentPool *const buffer_pool, const bool gc_enabled,
                     storage::LogManager *log_manager, uint32_t num_gc = 1)
      : buffer_pool_(buffer_pool), gc_enabled_(gc_enabled), log_manager_(log_manager), num_gc_(num_gc) {
    for (uint32_t i = 0; i < num_gc; i++)
      completed_txns_.emplace_back();
  }

  /**
   * Begins a transaction.
   * @return transaction context for the newly begun transaction
   */
  TransactionContext *BeginTransaction();

  /**
   * Commits a transaction, making all of its changes visible to others.
   * @param txn the transaction to commit
   * @param callback function pointer of the callback to invoke when commit is
   * @param callback_arg a void * argument that can be passed to the callback function when invoked
   * @return commit timestamp of this transaction
   */
  timestamp_t Commit(TransactionContext *txn, transaction::callback_fn callback, void *callback_arg);

  /**
   * Aborts a transaction, rolling back its changes (if any).
   * @param txn the transaction to abort.
   */
  void Abort(TransactionContext *txn);

  /**
   * Get the oldest transaction alive in the system at this time. Because of concurrent operations, it
   * is not guaranteed that upon return the txn is still alive. However, it is guaranteed that the return
   * timestamp is older than any transactions live.
   * @return timestamp that is older than any transactions alive
   */
  timestamp_t OldestTransactionStartTime() const;

  /**
   * @return unique timestamp based on current time, and advances one tick
   */
  timestamp_t GetTimestamp() { return time_++; }

  /**
   * @return true if gc_enabled and storing completed txns in local queue, false otherwise
   */
  bool GCEnabled() const { return gc_enabled_; }

  /**
   * Return a copy of the completed txns queue and empty the local version
   * @return copy of the completed txns for the GC to process
   */
  TransactionQueue CompletedTransactionsForGC(int gc_id);

  timestamp_t Protect() {
    timestamp_t start_time = time_++;
    common::SpinLatch::ScopedSpinLatch running_guard(&curr_running_txns_latch_);
    curr_running_txns_.emplace(start_time);
    return start_time;
  }

  void Release(timestamp_t t) {
    common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
    curr_running_txns_.erase(t);
  }

 private:
  storage::RecordBufferSegmentPool *buffer_pool_;
  // TODO(Tianyu): Timestamp generation needs to be more efficient (batches)
  // TODO(Tianyu): We don't handle timestamp wrap-arounds. I doubt this would be an issue though.
  std::atomic<timestamp_t> time_{timestamp_t(0)};
  // TODO(Tianyu): This is the famed HyPer Latch. We will need to re-evaluate performance later.
  common::SharedLatch commit_latch_;
  // TODO(Matt): consider a different data structure if this becomes a measured bottleneck
  std::unordered_set<timestamp_t> curr_running_txns_;
  mutable common::SpinLatch curr_running_txns_latch_;
  bool gc_enabled_ = false;
  storage::LogManager *const log_manager_;

  std::vector<TransactionQueue> completed_txns_;
  uint32_t num_gc_;

  timestamp_t ReadOnlyCommitCriticalSection(TransactionContext *txn, transaction::callback_fn callback,
                                            void *callback_arg);

  timestamp_t UpdatingCommitCriticalSection(TransactionContext *txn, transaction::callback_fn callback,
                                            void *callback_arg);

  void LogCommit(TransactionContext *txn, timestamp_t commit_time, transaction::callback_fn callback,
                 void *callback_arg);

  void Rollback(TransactionContext *txn, const storage::UndoRecord &record) const;

  void DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                      uint16_t projection_list_index,
                                      const storage::TupleAccessStrategy &accessor) const;

  void DeallocateInsertedTupleIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                       const storage::TupleAccessStrategy &accessor) const;
  void GCLastUpdateOnAbort(TransactionContext *txn);

  static uint32_t HashTxn(TransactionContext *txn);
};
}  // namespace terrier::transaction
