#include <stdio.h>
#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "common/typedefs.h"
#include "loggers/main_logger.h"
#include "loggers/storage_logger.h"
#include "loggers/transaction_logger.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/modeling_benchmark_util.h"
#include "util/storage_test_util.h"
#include "util/test_thread_pool.h"

#define LOG_FILE_NAME "concurrency_benchmark.log"
#define CSV_FILE_NAME "concurrency_benchmark.csv"

namespace terrier {

// This benchmark simulates the concurrent query execution with different mixtures of operations in transactions.
// We are interested in the system's raw performance, so the tuple's contents are intentionally left garbage and we
// don't verify correctness. That's the job of the Google Tests.

class ContentionBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    LOG_INFO("Setup once.\n");

    if (enable_gc_and_wal_ == true) {
      log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    }
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, enable_gc_and_wal_, log_manager_);

    if (enable_gc_and_wal_ == true) {
      // start logging and GC threads
      StartLogging(10);
      StartGC(txn_manager_, 10);
    }

    csv_file_ = fopen(CSV_FILE_NAME, "a");
  }

  void TearDown(const benchmark::State &state) final {
    if (enable_gc_and_wal_ == true) {
      EndGC();
      EndLogging();

      delete log_manager_;
    }
    delete txn_manager_;
    task_queues_.clear();

    fclose(csv_file_);
  }

  // The csv file for the experiment results
  FILE *csv_file_;

  // Workload
  const uint32_t num_operations_ = 1000000;
  const uint64_t buffer_pool_reuse_limit_ = 10000000;
  // Number of transactions per second
  const uint64_t txn_rates_ = 1000000;
  // Throttling threshold for the work queue size
  const uint64_t work_queue_threshold_ = 100;

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

  typedef std::chrono::high_resolution_clock::time_point time_point;

  // Latches to protect the worker queues
  // We have to define the size of this vector ahead of time becuase of c++ constraints, so we just define a very large
  // size.
  std::vector<common::SpinLatch> task_queue_latches_{100};
  // One task queue per worker thead
  std::vector<std::queue<time_point>> task_queues_;

  std::thread task_submitting_thread_;
  bool task_submitting_;

  void StartTaskSubmitting(uint64_t task_submit_period_nano) {
    task_submitting_ = true;
    // LOG_INFO("start submitting");
    task_submitting_thread_ =
        std::thread([task_submit_period_nano, this] { TaskSubmitThreadLoop(task_submit_period_nano); });
  }

  void EndTaskSubmitting() { task_submitting_thread_.join(); }

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

  void TaskSubmitThreadLoop(uint64_t task_submit_period_nano) {
    auto start = std::chrono::high_resolution_clock::now();
    time_point current;
    std::chrono::duration<uint64_t, std::nano> diff;
    uint64_t submitted_count = 0;
    uint64_t shortest_queue_size;
    uint32_t shortest_queue_id;
    auto num_threads = task_queues_.size();

    while (submitted_count < num_operations_) {
      current = std::chrono::high_resolution_clock::now();
      diff = current - start;
      if (diff.count() > submitted_count * task_submit_period_nano) {
        submitted_count++;
        shortest_queue_size = task_queues_[0].size();
        shortest_queue_id = 0;
        for (uint32_t i = 1; i < num_threads; ++i) {
          auto queue_size = task_queues_[i].size();
          if (queue_size < shortest_queue_size) {
            shortest_queue_size = queue_size;
            shortest_queue_id = i;
          }
        }
        // Reject txns if the queue is larger than 100
        if (shortest_queue_size > work_queue_threshold_) {
          // LOG_INFO("Rejecting 1!");
          continue;
        }

        task_queue_latches_[shortest_queue_id].Lock();
        task_queues_[shortest_queue_id].emplace(current);
        // LOG_INFO("add one item in thread {}, current size {}!", shortest_queue_id,
        //         task_queues_[shortest_queue_id].size());
        task_queue_latches_[shortest_queue_id].Unlock();
      }
      // std::this_thread::sleep_for(std::chrono::nanoseconds(task_submit_period_nano / 10));
    }
    task_submitting_ = false;
    // LOG_INFO("finished submitting!");
  }
};

static void CustomArguments(benchmark::internal::Benchmark *b) {
  for (int i = 1; i <= 16; i *= 2)
    for (int j = 1; j <= 9; j += 2)
      for (int k = 0; k <= 100; k += 10)
        for (int l = 0; l <= 100 - k; l += 10)
          for (int m = 8; m <= 16; m += 8) b->Args({i, j, k, l, m});
}

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(ContentionBenchmark, RunBenchmark)(benchmark::State &state) {
  TestThreadPool thread_pool;

  uint64_t total_latency(0);
  uint64_t total_committed(0);
  uint64_t total_aborted(0);
  uint64_t total_blocks_latch_wait(0);
  uint64_t total_bitmap_wait(0);
  uint64_t total_elapsed_ms(0);

  const uint32_t num_threads = state.range(0);
  const uint32_t txn_length = state.range(1);
  const uint32_t insert_percenrage = state.range(2);
  const uint32_t update_percenrage = state.range(3);
  const uint32_t num_attrs = state.range(4);

  UNUSED_ATTRIBUTE const uint32_t select_percenrage = 100 - insert_percenrage - update_percenrage;
  const std::vector<double> insert_update_select_ratio = {insert_percenrage / 100.0, update_percenrage / 100.0,
                                                          select_percenrage / 100.0};
  // const std::vector<double> insert_update_select_ratio = {0, 1, 0};
  const std::vector<uint8_t> attr_sizes(num_attrs, 8);
  // Initialize the data structure for task queues
  for (uint32_t i = 0; i < num_threads; ++i) {
    task_queues_.emplace_back(std::queue<time_point>());
  }
  const uint32_t initial_table_size = 1000000;

  // NOLINTNEXTLINE
  for (auto _ : state) {
    LOG_INFO("Run once.");
    ModelingBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, insert_update_select_ratio,
                                   &block_store_, &buffer_pool_, &generator_, task_submitting_, task_queues_,
                                   task_queue_latches_, enable_gc_and_wal_, txn_manager_, log_manager_);
    total_blocks_latch_wait -= tested.GetTotalBlocksLatchWait();
    total_bitmap_wait -= tested.GetTotalBitmapWait();
    StartTaskSubmitting(1000000000 / txn_rates_);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      tested.SimulateOltp();
    }
    EndTaskSubmitting();

    total_committed += tested.GetCommitCount();
    total_aborted += tested.GetAbortCount();
    total_latency += tested.GetLatencyCount();
    total_blocks_latch_wait += tested.GetTotalBlocksLatchWait();
    total_bitmap_wait += tested.GetTotalBitmapWait();

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    total_elapsed_ms += elapsed_ms;

    LOG_INFO("Committed: {} Time: {}", tested.GetCommitCount() / 1000, elapsed_ms);
  }

  state.SetItemsProcessed(total_committed);

  auto total_txn_num = total_committed + total_aborted;
  LOG_INFO("Committed: {} Aborted: {}", total_committed, total_aborted);
  LOG_INFO("Average throughput: {} k/s", total_committed / total_elapsed_ms);
  LOG_INFO("Average latency: {}", total_latency / total_txn_num);
  LOG_INFO("Average commit latch wait: {}", txn_manager_->GetTotalCommitLatchWait() / total_txn_num);
  LOG_INFO("Average table latch wait: {}", txn_manager_->GetTotalTableLatchWait() / total_txn_num);
  LOG_INFO("Average blocks latch wait: {}", total_blocks_latch_wait / total_txn_num);
  LOG_INFO("Average concurrent bitmap wait: {}", total_bitmap_wait / total_txn_num);

  // log the params
  fprintf(csv_file_, "%d,%d,%d,%d,%d", num_threads, txn_length, insert_percenrage, update_percenrage, num_attrs);
  // log the results
  fprintf(csv_file_, ",%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld\n", total_committed, total_aborted,
          total_committed / total_elapsed_ms, total_latency / total_txn_num,
          txn_manager_->GetTotalCommitLatchWait() / total_txn_num,
          txn_manager_->GetTotalTableLatchWait() / total_txn_num, total_blocks_latch_wait / total_txn_num,
          total_bitmap_wait / total_txn_num);
  fflush(csv_file_);
}

BENCHMARK_REGISTER_F(ContentionBenchmark, RunBenchmark)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->MinTime(5)
    ->Apply(CustomArguments);

}  // namespace terrier
