#include <list>
#include "storage/storage_defs.h"

int do_rdma(int sockfd, std::list<terrier::storage::RawBlock *> blocks);