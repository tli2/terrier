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
    // LOG_INFO("Setup once.\n");
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);

    if (enable_gc_and_wal_ == true) {
      log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    }
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, enable_gc_and_wal_, log_manager_);

    if (enable_gc_and_wal_ == true) {
      // start logging and GC threads
      StartLogging(10);
      StartGC(txn_manager_, 10);
    }
  }

  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
    if (enable_gc_and_wal_ == true) {
      EndGC();
      EndLogging();

      delete log_manager_;
    }
    delete txn_manager_;
  }

  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_, column_size_, column_size_,
                                      column_size_, column_size_, column_size_, column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};

  // Workload
  const uint32_t num_inserts_ = 1000000;
  const uint32_t num_reads_ = 1000000;
  // const uint32_t num_threads_ = TestThreadPool::HardwareConcurrency();
  const uint32_t num_threads_ = 4;
  const uint64_t buffer_pool_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  // storage::RecordBufferSegmentPool buffer_pool_{1000, 100};
  storage::RecordBufferSegmentPool buffer_pool_{buffer_pool_reuse_limit_, buffer_pool_reuse_limit_};
  std::thread log_thread_;
  bool logging_;
  volatile bool run_gc_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_;
  transaction::TransactionManager *txn_manager_;
  storage::LogManager *log_manager_ = LOGGING_DISABLED;

  bool enable_gc_and_wal_ = false;

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // The data table used in the experiments
  storage::DataTable table_{&block_store_, layout_, layout_version_t(0)};

 private:
  void StartLogging(uint32_t log_period_milli) {
    logging_ = true;
    log_thread_ = std::thread([log_period_milli, this] { LogThreadLoop(log_period_milli); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_->Shutdown();
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
      log_manager_->Process();
    }
  }

  void GCThreadLoop(uint32_t gc_period_milli) {
    while (run_gc_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_period_milli));
      gc_->PerformGarbageCollection();
    }
  }
};

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentInsert)(benchmark::State &state) {
  TestThreadPool thread_pool;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; i++) {
        // auto start = std::chrono::high_resolution_clock::now();

        // Insert buffer pointers
        byte *redo_buffer;
        storage::ProjectedRow *redo;

        redo_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
        redo = initializer_.InitializeRow(redo_buffer);
        StorageTestUtil::PopulateRandomRow(redo, layout_, 0, &generator_);

        auto *txn = txn_manager_->BeginTransaction();
        auto inserted = table_.Insert(txn, *redo);
        txn_manager_->Commit(txn, [] {});

        if (enable_gc_and_wal_ == true) {
          auto *record = txn->StageWrite(nullptr, inserted, initializer_);
          TERRIER_MEMCPY(record->Delta(), redo, redo->Size());
        }

        delete[] redo_buffer;
        // delete[] reinterpret_cast<byte *>(redo);
      }
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentRandomRead)(benchmark::State &state) {
  TestThreadPool thread_pool;
  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_, LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_.Insert(&txn, *redo_));
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
        // Read buffer pointers;
        byte *read_buffer;
        storage::ProjectedRow *read;

        // generate a ProjectedRow buffer to Read
        read_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
        read = initializer_.InitializeRow(read_buffer);

        auto *txn = txn_manager_->BeginTransaction();
        table_.Select(txn, read_order[(rand_read_offsets[id] + i) % read_order.size()], read);
        txn_manager_->Commit(txn, [] {});

        delete[] read_buffer;
        // delete[] reinterpret_cast<byte *>(read);
      }
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ConcurrencyBenchmark, ConcurrentRandomUpdate)(benchmark::State &state) {
  TestThreadPool thread_pool;
  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_, LOGGING_DISABLED);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(table_.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  std::uniform_int_distribution<uint32_t> rand_start(0, static_cast<uint32_t>(read_order.size() - 1));
  std::vector<uint32_t> rand_read_offsets;
  for (uint32_t i = 0; i < num_threads_; ++i) {
    // Create random reads
    rand_read_offsets.emplace_back(rand_start(generator_));
  }

  // Number of aborted transactions
  std::atomic<uint32_t> num_aborts;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      for (uint32_t i = 0; i < num_reads_ / num_threads_; i++) {
        // Update buffer pointers
        byte *redo_buffer;
        storage::ProjectedRow *redo;

        redo_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
        redo = initializer_.InitializeRow(redo_buffer);
        StorageTestUtil::PopulateRandomRow(redo, layout_, 0, &generator_);

        auto *txn = txn_manager_->BeginTransaction();
        auto update_slot = read_order[(rand_read_offsets[id] + i) % read_order.size()];
        if (enable_gc_and_wal_ == true) {
          auto *record = txn->StageWrite(nullptr, update_slot, initializer_);
          TERRIER_MEMCPY(record->Delta(), redo_, redo_->Size());
        }
        bool update_result = table_.Update(txn, update_slot, *redo);
        if (update_result == false) {
          LOG_INFO("Aborting because update failure!!\n");
          txn_manager_->Abort(txn);
          num_aborts += 1;
        } else {
          txn_manager_->Commit(txn, [] {});
        }

        delete[] redo_buffer;
        // delete[] reinterpret_cast<byte *>(redo);
      }
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_ - num_aborts);
}

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentRandomRead)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(ConcurrencyBenchmark, ConcurrentRandomUpdate)->Unit(benchmark::kMillisecond)->UseRealTime();
}  // namespace terrier
