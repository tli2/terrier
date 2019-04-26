#pragma once
#include "storage/data_table.h"
#include "network/rdma/rdma.h"

int do_rdma(int sockfd, terrier::storage::DataTable *datatable);
int do_send(struct resources *res, char *buf, size_t buf_size, uint64_t remote_addr);