#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/typedefs.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_thread_pool.h"

#define LOG_FILE_NAME "concurrency_benchmark.log"

namespace terrier {

// This benchmark simulates the concurrent query execution in a YCSB-like benchmark.
// We are interested in the system's raw performance, so the tuple's contents are intentionally left garbage and we
// don't verify correctness. That's the job of the Google Tests.

class ConcurrencyBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    init_main_logger();
    // initialize namespace specific loggers
    terrier::storage::init_storage_logger();
    terrier::transaction::init_transaction_logger();

    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);

    // generate a ProjectedRow buffer to Read
    read_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    read_ = initializer_.InitializeRow(read_buffer_);

    // generate a vector of ProjectedRow buffers for concurrent reads
    for (uint32_t i = 0; i < num_threads_; ++i) {
      // Create read buffer
      byte *read_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
      storage::ProjectedRow *read = initializer_.InitializeRow(read_buffer);
      read_buffers_.emplace_back(read_buffer);
      reads_.emplace_back(read);
    }

    // start logging and GC threads
    StartLogging(10);
    StartGC(&txn_manager_, 10);
  }

  void TearDown(const benchmark::State &state) final {
    EndGC();
    EndLogging();

    delete[] redo_buffer_;
    delete[] read_buffer_;
    for (auto ptr : loose_txns_) delete ptr;
    for (uint32_t i = 0; i < num_threads_; ++i) delete[] read_buffers_[i];
    // google benchmark might run benchmark several iterations. We need to clear vectors.
    read_buffers_.clear();
    reads_.clear();
  }

  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_, column_size_, column_size_,
                                      column_size_, column_size_, column_size_, column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};

  // Workload
  const uint32_t num_inserts_ = 100;
  const uint32_t num_reads_ = 100;
  const uint32_t num_threads_ = TestThreadPool::HardwareConcurrency();
  const uint64_t buffer_pool_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  // storage::RecordBufferSegmentPool buffer_pool_{1000, 100};
  storage::RecordBufferSegmentPool buffer_pool_{buffer_pool_reuse_limit_, buffer_pool_reuse_limit_};
  storage::LogManager log_manager_{LOG_FILE_NAME, &buffer_pool_};
  std::thread log_thread_;
  bool logging_;
  volatile bool run_gc_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_;
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, &log_manager_};

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // Read buffer pointers;
  byte *read_buffer_;
  storage::ProjectedRow *read_;

  // Read buffers pointers for concurrent reads
  std::vector<byte *> read_buffers_;
  std::vector<storage::ProjectedRow *> reads_;

  // Keep track of the allocated transactions that need to free at the end
  std::vector<transaction::TransactionContext *> loose_txns_;

 private:
  void StartLogging(uint32_t log_period_milli) {
    logging_ = true;
    log_thread_ = std::thread([log_period_milli, this] { LogThreadLoop(log_period_milli); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_.Shutdown();
  }

  void StartGC(transaction::TransactionManager *txn_manager, uint32_t gc_period_milli) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([gc_period_milli, this] { GCThreadLoop(gc_period_milli); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  void LogThreadLoop(uint32_t log_period_milli) {
    while (logging_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
      log_manager_.Process();
    }
  }

  void GCThreadLoop(uint32_t gc_period_milli) {
    while (run_gc_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_period_milli));
      gc_->PerformGarbageCollection();
    }
  }
};

// Insert the num_inserts_ of tuples into a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, SimpleInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_, layout_version_t(0));
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      auto *txn = txn_manager_.BeginTransaction();
      // loose_txns_.push_back(txn);
      table.Insert(txn, *redo_);
      txn_manager_.Commit(txn, [] {});
      delete txn;
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentInsert)(benchmark::State &state) {
  TestThreadPool thread_pool;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_, layout_version_t(0));
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; i++) {
        auto *txn = txn_manager_.BeginTransaction();
        table.Insert(txn, *redo_);
        txn_manager_.Commit(txn, [] {});
        delete txn;
      }
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, SequentialRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // Populate read_table by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_, LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_reads_; ++i) {
      auto *txn = txn_manager_.BeginTransaction();
      read_table.Select(txn, read_order[i], read_);
      txn_manager_.Commit(txn, [] {});
      delete txn;
    }
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, RandomRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_, LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_reads_; ++i) {
      auto *txn = txn_manager_.BeginTransaction();
      read_table.Select(txn, read_order[i], read_);
      txn_manager_.Commit(txn, [] {});
      delete txn;
    }
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentRandomRead)(benchmark::State &state) {
  TestThreadPool thread_pool;
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_, LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_reads_ / num_threads_; i++) {
        auto *txn = txn_manager_.BeginTransaction();
        read_table.Select(txn, read_order[(rand_read_offsets[id] + i) % read_order.size()], reads_[id]);
        txn_manager_.Commit(txn, [] {});
        delete txn;
      }
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, RandomUpdate)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_, LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_reads_; ++i) {
      auto *txn = txn_manager_.BeginTransaction();
      read_table.Update(txn, read_order[i], *redo_);
      txn_manager_.Commit(txn, [] {});
      delete txn;
    }
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentRandomUpdate)(benchmark::State &state) {
  TestThreadPool thread_pool;
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_, LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_reads_ / num_threads_; i++) {
        auto *txn = txn_manager_.BeginTransaction();
        bool update_result =
            read_table.Update(txn, read_order[(rand_read_offsets[id] + i) % read_order.size()], *redo_);
        if (update_result == false) {
          printf("Aborting because update failure!!\n");
          fflush(stdout);
          txn_manager_.Abort(txn);
        } else {
          txn_manager_.Commit(txn, [] {});
        }
        delete txn;
      }
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, SequentialRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, RandomRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentRandomRead)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, RandomUpdate)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentRandomUpdate)->Unit(benchmark::kMillisecond)->UseRealTime();
}  // namespace terrier
