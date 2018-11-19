#pragma once
#include <vector>
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {
class GarbageCollector;
}

namespace terrier::transaction {
/**
 * A transaction context encapsulates the information kept while the transaction is running
 */
class TransactionContext {
 public:
  /**
   * Constructs a new transaction context. Beware that the buffer pool given must be the same one the log manager uses,
   * if logging is enabled.
   * // TODO(Tianyu): We can terrier assert the above condition, but I need to go figure out friends.
   * @param start the start timestamp of the transaction
   * @param txn_id the id of the transaction, should be larger than all start time and commit time
   * @param buffer_pool the buffer pool to draw this transaction's undo buffer from
   * @param log_manager pointer to log manager in the system, or nullptr, if logging is disabled
   */
  TransactionContext(const timestamp_t start, const timestamp_t txn_id,
                     storage::RecordBufferSegmentPool *const buffer_pool, storage::LogManager *const log_manager,
                     bool enable_contention_metrics = false)
      : start_time_(start),
        txn_id_(txn_id),
        undo_buffer_(buffer_pool),
        redo_buffer_(log_manager, buffer_pool),
        enable_contention_metrics_(enable_contention_metrics) {}

  /**
   * @return start time of this transaction
   */
  timestamp_t StartTime() const { return start_time_; }

  /**
   * @return id of this transaction
   */
  const std::atomic<timestamp_t> &TxnId() const { return txn_id_; }

  /**
   * @return id of this transaction
   */
  std::atomic<timestamp_t> &TxnId() { return txn_id_; }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the update given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being updated
   * @param redo the content of the update
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForUpdate(storage::DataTable *const table, const storage::TupleSlot slot,
                                           const storage::ProjectedRow &redo) {
    const uint32_t size = storage::UndoRecord::Size(redo);
    return storage::UndoRecord::InitializeUpdate(undo_buffer_.NewEntry(size), txn_id_.load(), slot, table, redo);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the insert given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot inserted
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForInsert(storage::DataTable *const table, const storage::TupleSlot slot) {
    byte *result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
    return storage::UndoRecord::InitializeInsert(result, txn_id_.load(), slot, table);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the delete given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being deleted
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForDelete(storage::DataTable *const table, const storage::TupleSlot slot) {
    byte *result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
    return storage::UndoRecord::InitializeDelete(result, txn_id_.load(), slot, table);
  }

  /**
   * Expose a record that can hold a change, described by the initializer given, that will be logged out to disk.
   * The change can either be copied into this space, or written in the space and then used to change the DataTable.
   * // TODO(Matt): this isn't ideal for Insert since have to call that first and then log it after have a TupleSlot,
   * but it is safe and correct from WAL standpoint
   * @param table the DataTable that this record changes
   * @param slot the slot that this record changes
   * @param initializer the initializer to use for the underlying record
   * @return pointer to the initialized redo record.
   */
  storage::RedoRecord *StageWrite(storage::DataTable *const table, const storage::TupleSlot slot,
                                  const storage::ProjectedRowInitializer &initializer) {
    uint32_t size = storage::RedoRecord::Size(initializer);
    auto *log_record =
        storage::RedoRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, table, slot, initializer);
    return log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
  }

  /**
   * Initialize a record that logs a delete, that will be logged out to disk
   * @param table the DataTable that this record changes
   * @param slot the slot that this record changes
   */
  void StageDelete(storage::DataTable *const table, const storage::TupleSlot slot) {
    uint32_t size = storage::DeleteRecord::Size();
    storage::DeleteRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, table, slot);
  }

  typedef std::chrono::high_resolution_clock::time_point time_point;

  /**
   * @return whether the contention metrics is enabled to collect for this transaction
   */
  bool EnableContentionMetrics() const { return enable_contention_metrics_; }

  /**
   * Add the wait time duration for one acquisition to the commit latch metrics
   */
  void AddToCommitLatch(uint64_t duration) {
    commit_latch_wait_ += duration;
    commit_latch_count_++;
  }

  /*
   * @return The total wait time on the commit latch (nanoseconds)
   */
  uint64_t GetCommitLatchWait() const { return commit_latch_wait_; }

  /*
   * @return The total number of acquisition on the commit latch
   */
  uint64_t GetCommitLatchCount() const { return commit_latch_count_; }

  /**
   * Add the wait time duration for one acquisition to the table latch metrics
   */
  void AddToTableLatch(uint64_t duration) {
    table_latch_wait_ += duration;
    table_latch_count_++;
  }

  /*
   * @return The total wait time on the table latch (nanoseconds)
   */
  uint64_t GetTableLatchWait() const { return table_latch_wait_; }

  /*
   * @return The total number of acquisition on the table latch
   */
  uint64_t GetTableLatchCount() const { return table_latch_count_; }

  /**
   * Add the wait time duration for one acquisition to the block latch metrics
   */
  void AddToBlockLatch(uint64_t duration) {
    block_latch_wait_ += duration;
    block_latch_count_++;
  }

  /*
   * @return The total wait time on the block latch (nanoseconds)
   */
  uint64_t GetBlockLatchWait() const { return block_latch_wait_; }

  /*
   * @return The total number of acquisition on the block latch
   */
  uint64_t GetBlockLatchCount() const { return block_latch_count_; }

  /**
   * Add the wait time duration for one acquisition to the bitmap latch metrics
   */
  void AddToBitmapLatch(uint64_t duration) {
    bitmap_latch_wait_ += duration;
    bitmap_latch_count_++;
  }

  /*
   * @return The total wait time on the bitmap latch (nanoseconds)
   */
  uint64_t GetBitmapLatchWait() const { return bitmap_latch_wait_; }

  /*
   * @return The total number of acquisition on the bitmap latch
   */
  uint64_t GetBitmapLatchCount() const { return bitmap_latch_count_; }

 private:
  friend class storage::GarbageCollector;
  friend class TransactionManager;
  const timestamp_t start_time_;
  std::atomic<timestamp_t> txn_id_;
  storage::UndoBuffer undo_buffer_;
  storage::RedoBuffer redo_buffer_;

  // Whether to enable the tracking of contention related metrics
  bool enable_contention_metrics_;

  // total wait time on the commit latch for this transaction (nanoseconds)
  uint64_t commit_latch_wait_{0};
  // total number of acquisition on the commit latch for this transaction (nanoseconds)
  uint64_t commit_latch_count_{0};

  // total wait time on the table latch for this transaction (nanoseconds)
  uint64_t table_latch_wait_{0};
  // total number of acquisition on the table latch for this transaction (nanoseconds)
  uint64_t table_latch_count_{0};

  // total wait time on the concurrent bitmap for this transaction (nanoseconds)
  uint64_t bitmap_latch_wait_{0};
  // total number of acquisition on the concurrent bitmap for this transaction (nanoseconds)
  uint64_t bitmap_latch_count_{0};

  // total wait time on the block latch for this transaction (nanoseconds)
  uint64_t block_latch_wait_{0};
  // total number of acquisition on the block latch for this transaction (nanoseconds)
  uint64_t block_latch_count_{0};
};
}  // namespace terrier::transaction
