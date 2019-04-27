//#include <arrow/flight/api.h>
//#include <arrow/table.h>
//#include <arrow/util/logging.h>
//#include "common/macros.h"
//
//int main() {
//  std::unique_ptr<arrow::flight::FlightClient> read_client;
//  ARROW_CHECK_OK(arrow::flight::FlightClient::Connect("127.0.0.1", 15712, &read_client));
//  arrow::flight::Ticket ticket{};
//  std::unique_ptr<arrow::RecordBatchReader> stream;
//  ARROW_CHECK_OK(read_client->DoGet(ticket, &stream));
//  std::shared_ptr<arrow::Table> retrieved_data;
//  std::vector<std::shared_ptr<arrow::RecordBatch>> retrieved_chunks;
//  std::shared_ptr<arrow::RecordBatch> chunk;
//  while (true) {
//    ARROW_CHECK_OK(stream->ReadNext(&chunk));
//    if (chunk == nullptr) break;
//    retrieved_chunks.push_back(chunk);
//  }
//  std::shared_ptr<arrow::Schema> schema = stream->schema();
//  ARROW_CHECK_OK(arrow::Table::FromRecordBatches(schema, retrieved_chunks, &retrieved_data));
//  UNUSED_ATTRIBUTE volatile void *foo = retrieved_data.get();
//  return 0;
//}
