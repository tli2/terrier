#include <utility>
#include <vector>
#include <random>
#include <arrow/api.h>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector.h"
#include "util/storage_test_util.h"
#include "storage/block_compactor.h"

namespace terrier {
class BlockCompactorBenchmark : public benchmark::Fixture {
 public:
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  storage::BlockLayout layout_{{8, 8, VARLEN_COLUMN}};
  storage::TupleAccessStrategy accessor_{layout_};
  std::default_random_engine generator_;
  storage::DataTable table_{&block_store_, layout_, storage::layout_version_t(0)};
  transaction::TransactionManager txn_manager_{&buffer_pool_, true, LOGGING_DISABLED};
  storage::GarbageCollector gc_{&txn_manager_};
  storage::BlockCompactor compactor_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, NoTxn)(benchmark::State &state) {
  std::vector<storage::RawBlock *> start_blocks;
  for (uint32_t i = 0; i < 100; i++) {
    storage::RawBlock *block = block_store_.Get();
    StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, 0.05, &generator_);
    start_blocks.push_back(block);
  }

  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<storage::RawBlock *> blocks;
    for (storage::RawBlock *block : start_blocks) {
      storage::RawBlock *copied_block = block_store_.Get();
      std::memcpy(copied_block, block, common::Constants::BLOCK_SIZE);
      blocks.push_back(copied_block);
    }
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      arrow::Int64Builder int_builder;
      arrow::StringBuilder string_builder;
      for (storage::RawBlock *block : blocks) {
        auto *bitmap = accessor_.ColumnNullBitmap(block, storage::col_id_t(2));
        auto *buffer = reinterpret_cast<uint64_t *>(accessor_.ColumnStart(block, storage::col_id_t(2)));
        for (uint32_t i = 0; i < layout_.NumSlots(); i++) {
          if (!accessor_.Allocated({block, i})) continue;
          if (!bitmap->Test(i)) {
            auto status5 UNUSED_ATTRIBUTE = int_builder.AppendNull();
          }
          else {
            auto status6 UNUSED_ATTRIBUTE = int_builder.Append(buffer[i]);
          }
        }
        auto *bitmap1 = accessor_.ColumnNullBitmap(block, storage::col_id_t(1));
        auto *buffer1 = reinterpret_cast<storage::VarlenEntry *>(accessor_.ColumnStart(block, storage::col_id_t(1)));
        for (uint32_t i = 0; i < layout_.NumSlots(); i++) {
          if (!accessor_.Allocated({block, i})) continue;
          if (!bitmap1->Test(i))
            auto status UNUSED_ATTRIBUTE = string_builder.AppendNull();
          else {
            auto &entry = buffer1[i];
            auto status2 UNUSED_ATTRIBUTE = string_builder.Append(reinterpret_cast<const uint8_t *>(entry.Content()), entry.Size());
          }
        }
      }
      std::shared_ptr<arrow::Array> int_column, string_column;
      auto status3 UNUSED_ATTRIBUTE = int_builder.Finish(&int_column);
      auto status4 UNUSED_ATTRIBUTE = string_builder.Finish(&string_column);
      std::vector<std::shared_ptr<arrow::Field>> schema_vector{arrow::field("1", arrow::uint64()), arrow::field("2", arrow::utf8())};

      std::vector<std::shared_ptr<arrow::Array>> table_vector{int_column, string_column};
      volatile std::shared_ptr<arrow::Table>
          table = arrow::Table::Make(std::make_shared<arrow::Schema>(schema_vector), table_vector);
    }
    for (storage::RawBlock *block : blocks)
      block_store_.Release(block);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(static_cast<int64_t>(100 * state.iterations()));
}


// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, Strawman)(benchmark::State &state) {
  std::vector<storage::RawBlock *> start_blocks;
  for (uint32_t i = 0; i < 100; i++) {
    storage::RawBlock *block = block_store_.Get();
    StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, 0.5, &generator_);
    start_blocks.push_back(block);
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {

    std::vector<storage::RawBlock *> blocks;
    for (storage::RawBlock *block : start_blocks) {
      storage::RawBlock *copied_block = block_store_.Get();
      std::memcpy(copied_block, block, common::Constants::BLOCK_SIZE);
      blocks.push_back(copied_block);
    }

    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      transaction::TransactionContext *txn = txn_manager_.BeginTransaction();
      storage::ProjectedRowInitializer initializer(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
      byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
      auto *read_row = initializer.InitializeRow(buffer);
      arrow::Int64Builder int_builder;
      arrow::StringBuilder string_builder;
      for (storage::RawBlock *block : blocks) {
        for (uint32_t i = 0; i < layout_.NumSlots(); i++) {
          storage::TupleSlot slot(block, i);
          bool visible = table_.Select(txn, slot, read_row);
          if (!visible) continue;
          auto *int_pointer = read_row->AccessWithNullCheck(1);
          if (int_pointer == nullptr)
            auto status5 UNUSED_ATTRIBUTE = int_builder.AppendNull();
          else
            auto status6 UNUSED_ATTRIBUTE = int_builder.Append(*reinterpret_cast<uint64_t *>(int_pointer));
          auto *varlen_pointer = read_row->AccessWithNullCheck(0);
          if (varlen_pointer == nullptr)
            auto status UNUSED_ATTRIBUTE = string_builder.AppendNull();
          else {
            auto *entry = reinterpret_cast<storage::VarlenEntry *>(varlen_pointer);
            auto status2 UNUSED_ATTRIBUTE = string_builder.Append(reinterpret_cast<const uint8_t *>(entry->Content()), entry->Size());
          }
        }
        std::shared_ptr<arrow::Array> int_column, string_column;
        auto status3 UNUSED_ATTRIBUTE = int_builder.Finish(&int_column);
        auto status4 UNUSED_ATTRIBUTE = string_builder.Finish(&string_column);
        std::vector<std::shared_ptr<arrow::Field>> schema_vector{arrow::field("1", arrow::uint64()), arrow::field("2", arrow::utf8())};

        std::vector<std::shared_ptr<arrow::Array>> table_vector{int_column, string_column};
        volatile std::shared_ptr<arrow::Table>
            table = arrow::Table::Make(std::make_shared<arrow::Schema>(schema_vector), table_vector);
      }
    }
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    for (storage::RawBlock *block : blocks)
      block_store_.Release(block);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(100 * static_cast<int64_t>(state.iterations()));
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, CompactionThroughput)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<storage::RawBlock *> blocks;
    for (uint32_t i = 0; i < 100; i++) {
      storage::RawBlock *block = block_store_.Get();
      StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, 0.05, &generator_);
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
    for (storage::RawBlock *block : blocks)
      compactor_.PutInQueue({block, &table_});
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      compactor_.ProcessCompactionQueue(&txn_manager_);
    }
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    for (storage::RawBlock *block : blocks)
      block_store_.Release(block);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(static_cast<int64_t>(100 * state.iterations()));
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BlockCompactorBenchmark, GatherThroughput)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    std::vector<storage::RawBlock *> blocks;
    for (uint32_t i = 0; i < 100; i++) {
      storage::RawBlock *block = block_store_.Get();
      StorageTestUtil::PopulateBlockRandomlyNoBookkeeping(layout_, block, 0.05, &generator_);
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
    for (storage::RawBlock *block : blocks)
      compactor_.PutInQueue({block, &table_});
    compactor_.ProcessCompactionQueue(&txn_manager_);
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    for (storage::RawBlock *block : blocks)
      compactor_.PutInQueue({block, &table_});
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      compactor_.ProcessCompactionQueue(&txn_manager_);
    }
//    gc_.PerformGarbageCollection();
//    gc_.PerformGarbageCollection();
    for (storage::RawBlock *block : blocks)
      block_store_.Release(block);
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(static_cast<int64_t>(100 * state.iterations()));
}

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, NoTxn)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(1);

//BENCHMARK_REGISTER_F(BlockCompactorBenchmark, Strawman)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(1);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, CompactionThroughput)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(1);

BENCHMARK_REGISTER_F(BlockCompactorBenchmark, GatherThroughput)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(1);


} // namespace terrier