#include "storage/block_compactor.h"
#include <vector>
#include "arrow/api.h"
#include "storage/block_access_controller.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {
struct BlockCompactorTest : public ::terrier::TerrierTest {
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1, 1};
};

// NOLINTNEXTLINE
TEST_F(BlockCompactorTest, SingleBlockCompactionTest) {
  // Initialize a block
  storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(100, &generator_);
  storage::TupleAccessStrategy accessor(layout);
  storage::RawBlock *block = block_store_.Get();

  accessor.InitializeRawBlock(block, storage::layout_version_t(0));

  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
  storage::DataTable table(&block_store_, layout, storage::layout_version_t(0));
  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
  // Enable GC to cleanup transactions started by the block compactor
  transaction::TransactionManager txn_manager(&buffer_pool, true, LOGGING_DISABLED);
  storage::GarbageCollector gc(&txn_manager);

  // Populate the block and set the column types
  auto tuples = StorageTestUtil::PopulateBlockRandomly(layout, block, 0.1, &generator_);
  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);
  for (storage::col_id_t col_id : layout.AllColumns()) {
    if (layout.IsVarlen(col_id)) {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::GATHERED_VARLEN;
    } else {
      arrow_metadata.GetColumnInfo(layout, col_id).Type() = storage::ArrowColumnType::FIXED_LENGTH;
    }
  }

  // Compact the block
  storage::BlockCompactor compactor;
  compactor.PutInQueue({block, &table});
  compactor.ProcessCompactionQueue(&txn_manager);  // should always succeed with no other threads

  EXPECT_EQ(storage::BlockState::COOLING, block->controller_.CurrentBlockState());
  // Verify the content of the block
  // This transaction is guaranteed to start after the compacting one commits
  storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::CreateProjectedRowInitializer(layout, StorageTestUtil::ProjectionListAllColumns(layout));
  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *read_row = initializer.InitializeRow(buffer);
  std::vector<storage::ProjectedRow *> moved_rows;
  transaction::TransactionContext *txn = txn_manager.BeginTransaction();
  auto num_tuples = tuples.size();
  // For non moved rows
  for (uint32_t i = 0; i < layout.NumSlots(); i++) {
    storage::TupleSlot slot(block, i);
    bool visible = table.Select(txn, slot, read_row);
    EXPECT_TRUE(visible ^ (i >= num_tuples));
  }
  txn_manager.Commit(txn, [](void *) -> void {}, nullptr);
  gc.PerformGarbageCollection();

  compactor.PutInQueue({block, &table});
  compactor.ProcessCompactionQueue(&txn_manager);

  txn = txn_manager.BeginTransaction();
  for (uint32_t i = 0; i < layout.NumSlots(); i++) {
    storage::TupleSlot slot(block, i);
    bool visible = table.Select(txn, slot, read_row);
    if (i >= num_tuples) {
      EXPECT_FALSE(visible);  // Should be deleted after compaction
    } else {
      EXPECT_TRUE(visible);  // Should be filled after compaction
      auto it = tuples.find(slot);
      if (it != tuples.end()) {
        // Here we can assume that the row is not moved. Check that everything is still equal. Has to be deep
        // equality because varlens are moved.
        EXPECT_TRUE(StorageTestUtil::ProjectionListEqualDeep(layout, it->second, read_row));
        for (storage::col_id_t col_id : layout.Varlens()) {
          // We know this equality to hold because the select looks at all the columns
          auto *entry = reinterpret_cast<storage::VarlenEntry *>(read_row->AccessWithNullCheck(static_cast<uint16_t>(!col_id - 1)));
          if (entry == nullptr) continue;
          // The varlen pointers should now point to within the Arrow data structure
          EXPECT_FALSE(entry->NeedReclaim());
        }
        delete[] reinterpret_cast<byte *>(tuples[slot]);
        tuples.erase(slot);
      } else {
        // Need to copy and do quadratic comparison later.
        byte *copied_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
        std::memcpy(copied_buffer, read_row, initializer.ProjectedRowSize());
        moved_rows.push_back(reinterpret_cast<storage::ProjectedRow *>(copied_buffer));
      }
    }
  }
  txn_manager.Commit(txn, [](void *) -> void {}, nullptr);  // Commit: will be cleaned up by GC

  // TODO(Tianyu): Also has to check that thte generated Arrow information is correct
  gc.PerformGarbageCollection();
  gc.PerformGarbageCollection();
  delete[] buffer;
  // TODO(Tianyu): Figure out delete
  // Deallocate arrow buffers
//  for (const auto &col_id : layout.AllColumns()) delete &arrow_metadata.Deallocate(layout, col_id);
  block_store_.Release(block);
}

// NOLINTNEXTLINE
// TEST_F(BlockCompactorTest, SingleBlockDictionaryTest) {
//  std::default_random_engine generator;
//  storage::BlockStore block_store{1, 1};
//  storage::RawBlock *block = block_store.Get();
//  storage::BlockLayout layout({8, 8, VARLEN_COLUMN});
//  storage::TupleAccessStrategy accessor(layout);
//  accessor.InitializeRawBlock(block, storage::layout_version_t(0));
//
//  // Technically, the block above is not "in" the table, but since we don't sequential scan that does not matter
//  storage::DataTable table(&block_store, layout, storage::layout_version_t(0));
//  storage::RecordBufferSegmentPool buffer_pool{10000, 10000};
//  // Enable GC to cleanup transactions started by the block compactor
//  transaction::TransactionManager txn_manager(&buffer_pool, true, LOGGING_DISABLED);
//  storage::GarbageCollector gc(&txn_manager);
//
//  auto tuples = StorageTestUtil::PopulateBlockRandomly(layout, block, 0.1, &generator);
//  auto &arrow_metadata = accessor.GetArrowBlockMetadata(block);
//  for (storage::col_id_t col_id : layout.AllColumns()) {
//    if (layout.IsVarlen(col_id)) {
//      arrow_metadata.GetColumnInfo(layout, col_id).type_ = storage::ArrowColumnType::GATHERED_VARLEN;
//    } else {
//      arrow_metadata.GetColumnInfo(layout, col_id).type_ = storage::ArrowColumnType::FIXED_LENGTH;
//    }
//  }
//
//  storage::BlockCompactor compactor;
//  compactor.PutInQueue({block, &table});
//  compactor.ProcessCompactionQueue(&txn_manager);  // should always succeed with no other threads
//
//  storage::ProjectedRowInitializer initializer(layout, StorageTestUtil::ProjectionListAllColumns(layout));
//  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
//  auto *read_row = initializer.InitializeRow(buffer);
//  std::vector<storage::ProjectedRow *> moved_rows;
//  // This transaction is guaranteed to start after the compacting one commits
//  transaction::TransactionContext *txn = txn_manager.BeginTransaction();
//  auto num_tuples = tuples.size();
//  for (uint32_t i = 0; i < layout.NumSlots(); i++) {
//    storage::TupleSlot slot(block, i);
//    bool visible = table.Select(txn, slot, read_row);
//    if (i >= num_tuples) {
//      EXPECT_FALSE(visible);  // Should be deleted after compaction
//    } else {
//      EXPECT_TRUE(visible);  // Should be filled after compaction
//      auto it = tuples.find(slot);
//      if (it != tuples.end()) {
//        // Here we can assume that the row is not moved. Check that everything is still equal. Has to be deep
//        // equality because varlens are moved.
//        EXPECT_TRUE(StorageTestUtil::ProjectionListEqualDeep(layout, it->second, read_row));
//        delete[] reinterpret_cast<byte *>(tuples[slot]);
//        tuples.erase(slot);
//      } else {
//        // Need to copy and do quadratic comparison later.
//        byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
//        std::memcpy(buffer, read_row, initializer.ProjectedRowSize());
//        moved_rows.push_back(reinterpret_cast<storage::ProjectedRow *>(buffer));
//      }
//    }
//  }
//  txn_manager.Commit(txn, [](void *) -> void {}, nullptr);  // Commit: will be cleaned up by GC
//  delete[] buffer;
//
//  for (auto *moved_row : moved_rows) {
//    bool match_found = false;
//    for (auto &entry : tuples) {
//      // This comparison needs to be deep because varlens are moved.
//      if (StorageTestUtil::ProjectionListEqualDeep(layout, entry.second, moved_row)) {
//        // Here we can assume that the row is not moved. All good.
//        delete[] reinterpret_cast<byte *>(entry.second);
//        tuples.erase(entry.first);
//        match_found = true;
//        break;
//      }
//    }
//    // the read tuple should be one of the original tuples that are moved.
//    EXPECT_TRUE(match_found);
//    delete[] reinterpret_cast<byte *>(moved_row);
//  }
//  // All tuples from the original block should have been accounted for.
//  EXPECT_TRUE(tuples.empty());
//  gc.PerformGarbageCollection();
//  gc.PerformGarbageCollection();  // Second call to deallocate.
//  // Deallocated arrow buffers
//  for (const auto &col_id : layout.AllColumns()) {
//    arrow_metadata.Deallocate(layout, col_id);
//  }
//  delete block;
//}

}  // namespace terrier