#include <arrow/api.h>
#include <random>
#include <utility>
#include <vector>
#include <memory>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/block_compactor.h"
#include "storage/garbage_collector.h"
#include "util/storage_test_util.h"

namespace terrier {
class BlockCompactorBenchmark : public benchmark::Fixture {
 protected:
  storage::BlockStore block_store_{1000, 1000};
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  storage::BlockLayout layout_{{8, VARLEN_COLUMN, VARLEN_COLUMN}};
  storage::TupleAccessStrategy accessor_{layout_};

  storage::DataTable table_{&block_store_, layout_, storage::layout_version_t(0)};
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
  storage::GarbageCollector gc_{&txn_manager_};
  storage::BlockCompactor compactor_;

  uint32_t num_blocks_ = 500;
  double percent_empty_ = 0.0;

};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, CompactionThroughput)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<storage::RawBlock *> blocks;
    for (uint32_t i = 0; i < num_blocks_; i++) {
      storage::RawBlock *block = block_store_.Get();
      StorageTestUtil::PopulateBlockRandomly(layout_, block, percent_empty_, &generator_);
      auto &arrow_metadata = accessor_.GetArrowBlockMetadata(block);
      for (storage::col_id_t col_id : layout_.AllColumns()) {
        if (layout_.IsVarlen(col_id)) {
          arrow_metadata.GetColumnInfo(layout_, col_id).type_ = storage::ArrowColumnType::GATHERED_VARLEN;
        } else {
          arrow_metadata.GetColumnInfo(layout_, col_id).type_ = storage::ArrowColumnType::FIXED_LENGTH;
        }
      }
      blocks.push_back(block);
    }
    // generate our table and instantiate GC
    for (storage::RawBlock *block : blocks) compactor_.PutInQueue({block, &table_});
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      compactor_.ProcessCompactionQueue(&txn_manager_);
    }
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    for (storage::RawBlock *block : blocks) block_store_.Release(block);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(static_cast<int64_t>(num_blocks_ * state.iterations()));
}

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, CompactionThroughput)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2)->Repetitions(5);

}  // namespace terrier
