#pragma once
#include <algorithm>
#include <unordered_map>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {
struct TestCallbacks {
  TestCallbacks() = delete;
  static void EmptyCallback(void * /*unused*/) {}
};

class ModelingBenchmarkObject;
class RandomTransaction;
using TupleEntry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;

/**
 * A RandomWorkloadTransaction class provides a simple interface to simulate a transaction running in the system.
 */
class RandomTransaction {
 public:
  /**
   * Initializes a new RandomWorkloadTransaction to work on the given test object
   * @param test_object the test object that runs this transaction
   */
  explicit RandomTransaction(ModelingBenchmarkObject *test_object);

  /**
   * Destructs a random workload transaction
   */
  ~RandomTransaction();

  /**
   * Randomly updates a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomUpdate(Random *generator);

  /**
   * Randomly inserts a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomInsert(Random *generator);

  /**
   * Randomly selects a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomSelect(Random *generator);

  /**
   * Finish the simulation of this transaction. The underlying transaction will either commit or abort.
   */
  void Finish(transaction::callback_fn callback, void *callback_arg);

  timestamp_t BeginTimestamp() const { return start_time_; }

  timestamp_t CommitTimestamp() const {
    if (aborted_) return timestamp_t(static_cast<uint64_t>(-1));
    return commit_time_;
  }

  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> *Updates() { return &updates_; }

 private:
  friend class ModelingBenchmarkObject;
  ModelingBenchmarkObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for correctness checks
  bool aborted_;
  timestamp_t start_time_, commit_time_;
  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> updates_;
  byte *buffer_;
};

/**
 * A LargeTransactionTest bootstraps a table, and runs randomly generated workloads concurrently against the table to
 * simulate a real run of the system. This works with or without gc.
 *
 * So far we only do updates and selects, as inserts and deletes are not given much special meaning without the index.
 */
class ModelingBenchmarkObject {
 public:
  typedef std::chrono::high_resolution_clock::time_point time_point;

  /**
   * Initializes a test object with the given configuration
   * @param max_columns the max number of columns in the generated test table
   * @param initial_table_size number of tuples the table should have
   * @param txn_length length of every simulated transaction, in number of operations (select or update)
   * @param update_select_ratio the ratio of inserts vs. updates vs. select in the generated transaction
   *                             (e.g. {0.0, 0.3, 0.7} will be 0% inserts, 30% updates, and 70% reads)
   * @param block_store the block store to use for the underlying data table
   * @param buffer_pool the buffer pool to use for simulated transactions
   * @param generator the random generator to use for the test
   * @param gc_on whether gc is enabled
   * @param log_manager pointer to the LogManager if enabled
   */
  ModelingBenchmarkObject(const std::vector<uint8_t> &attr_sizes, uint32_t initial_table_size, uint32_t txn_length,
                          std::vector<double> operation_ratio, storage::BlockStore *block_store,
                          storage::RecordBufferSegmentPool *buffer_pool, std::default_random_engine *generator,
                          bool &task_submitting, std::vector<std::queue<time_point>> &task_queues,
                          std::vector<common::SpinLatch> &task_queue_latches, bool gc_on,
                          transaction::TransactionManager *txn_manager,
                          storage::LogManager *log_manager = LOGGING_DISABLED);

  /**
   * Destructs a LargeTransactionBenchmarkObject
   */
  ~ModelingBenchmarkObject();

  /**
   * @return the transaction manager used by this test
   */
  transaction::TransactionManager *GetTxnManager() { return txn_manager_; }

  /**
   * Simulate an oltp workload.
   * Transactions are generated using the configuration provided on construction.
   */
  void SimulateOltp();

  /**
   * @return layout of the randomly generated table
   */
  const storage::BlockLayout &Layout() const { return layout_; }

  /**
   * @return the number of commited transactions
   */
  uint64_t GetCommitCount() const { return commit_count_; }

  /**
   * @return the number of aborted transactions
   */
  uint64_t GetAbortCount() const { return abort_count_; }

  /**
   * @return the number of total latency of transactions
   */
  uint64_t GetLatencyCount() const { return latency_count_; }

  /**
   * @return total wait time on the table's blocks latch in nanoseconds
   */
  uint64_t GetTotalBlocksLatchWait() { return table_.GetTotalBlocksLatchWait(); }

 private:
  void SimulateOneTransaction(RandomTransaction *txn, uint32_t txn_id, transaction::callback_fn callback,
                              void *callback_arg);

  template <class Random>
  void PopulateInitialTable(uint32_t num_tuples, Random *generator);

  friend class RandomTransaction;
  uint32_t txn_length_;
  std::vector<double> operation_ratio_;
  std::default_random_engine *generator_;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  transaction::TransactionManager *txn_manager_;
  transaction::TransactionContext *initial_txn_;
  bool gc_on_, wal_on_;

  bool &task_submitting_;
  std::vector<std::queue<time_point>> &task_queues_;
  std::vector<common::SpinLatch> &task_queue_latches_;

  uint64_t abort_count_;
  uint64_t commit_count_;
  uint64_t latency_count_;

  // tuple content is meaningless if bookkeeping is off.
  std::vector<TupleEntry> last_checked_version_;
  // so we don't have to calculate these over and over again
  storage::ProjectedRowInitializer row_initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};
};
}  // namespace terrier
