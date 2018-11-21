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

class ModelingBenchmarkObject;
class RandomTransaction;
using TupleEntry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;

/*
 * A wrapper class to store the metrics related to the contention benchmark
 */
class ContentionBenchmarkMetrics {
 public:
  std::atomic<uint64_t> total_latency_{0};
  std::atomic<uint64_t> total_committed_{0};
  std::atomic<uint64_t> total_aborted_{0};
  std::atomic<uint64_t> total_elapsed_ms_{0};

  std::atomic<uint64_t> total_commit_latch_wait_{0};
  std::atomic<uint64_t> total_commit_latch_count_{0};
  std::atomic<uint64_t> total_table_latch_wait_{0};
  std::atomic<uint64_t> total_table_latch_count_{0};
  std::atomic<uint64_t> total_bitmap_latch_wait_{0};
  std::atomic<uint64_t> total_bitmap_latch_count_{0};
  std::atomic<uint64_t> total_block_latch_wait_{0};
  std::atomic<uint64_t> total_block_latch_count_{0};
};

/*
 * A wrapper class to store the arguments used for the callback when txn commits
 */
class CommitCallbackArgs {
 public:
  CommitCallbackArgs(ContentionBenchmarkMetrics *metrics, transaction::TransactionContext::time_point start)
      : metrics_(metrics), start_(start) {}

  ContentionBenchmarkMetrics *metrics_;
  transaction::TransactionContext::time_point start_;
};

struct TestCallbacks {
  TestCallbacks() = delete;
  static void EmptyCallback(void * /*unused*/) {}

  static void CommitMetricsCallback(void *args) {
    auto callback_args = reinterpret_cast<CommitCallbackArgs *>(args);
    auto start = callback_args->start_;
    auto metrics = callback_args->metrics_;
    delete callback_args;
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<uint64_t, std::nano> diff = end - start;
    metrics->total_latency_ += diff.count();
    metrics->total_committed_++;
  };
};

/**
 * A RandomWorkloadTransaction class provides a simple interface to simulate a transaction running in the system.
 */
class RandomTransaction {
 public:
  /**
   * Initializes a new RandomWorkloadTransaction to work on the given test object
   * @param test_object the test object that runs this transaction
   */
  explicit RandomTransaction(ModelingBenchmarkObject *test_object, transaction::metrics_callback_fn metrics_callback);

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

  transaction::timestamp_t BeginTimestamp() const { return start_time_; }

  transaction::timestamp_t CommitTimestamp() const {
    if (aborted_) return transaction::timestamp_t(static_cast<uint64_t>(-1));
    return commit_time_;
  }

  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> *Updates() { return &updates_; }

  const transaction::TransactionContext *GetTransactionContext() const { return txn_; }

 private:
  friend class ModelingBenchmarkObject;
  ModelingBenchmarkObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for correctness checks
  bool aborted_;
  transaction::timestamp_t start_time_, commit_time_;
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
                          bool &task_submitting,
                          std::vector<std::queue<transaction::TransactionContext::time_point>> &task_queues,
                          std::vector<common::SpinLatch> &task_queue_latches, bool gc_on,
                          transaction::TransactionManager *txn_manager, storage::LogManager *log_manager);

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
  void SimulateOltp(ContentionBenchmarkMetrics *metrics);

  /**
   * @return layout of the randomly generated table
   */
  const storage::BlockLayout &Layout() const { return layout_; }

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
  std::vector<std::queue<transaction::TransactionContext::time_point>> &task_queues_;
  std::vector<common::SpinLatch> &task_queue_latches_;

  // tuple content is meaningless if bookkeeping is off.
  std::vector<TupleEntry> last_checked_version_;
  // so we don't have to calculate these over and over again
  storage::ProjectedRowInitializer row_initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};
};
}  // namespace terrier
