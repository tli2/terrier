#include <infiniband/verbs.h>
#include <sys/types.h>

/* structure of test parameters */
struct config_t
{
    const char *dev_name;         /* IB device name */
    char *server_name;            /* server host name */
    u_int32_t tcp_port;           /* server TCP port */
    int ib_port;                  /* local IB port to work with */
    int gid_idx;                  /* gid index to use */
};
/* structure to exchange data which is needed to connect the QPs */
struct cm_con_data_t
{
    uint64_t addr;                /* Buffer address */
    uint32_t rkey;                /* Remote key */
    uint32_t qp_num;              /* QP number */
    uint16_t lid;                 /* LID of the IB port */
    uint8_t gid[16];              /* gid */
} __attribute__ ((packed));
/* structure of system resources */
struct resources
{
    struct ibv_device_attr device_attr;
    /* Device attributes */
    struct ibv_port_attr port_attr;       /* IB port attributes */
    struct cm_con_data_t remote_props;    /* values to connect to remote side */
    struct ibv_context *ib_ctx;   /* device handle */
    struct ibv_pd *pd;            /* PD handle */
    struct ibv_cq *cq;            /* CQ handle */
    struct ibv_qp *qp;            /* QP handle */
    struct ibv_mr *mr;            /* MR handle for buf */
    char *buf;                    /* memory buffer pointer, used for RDMA and send
                                     ops */
    uint64_t size;                /* size of buf in bytes */
    int sock;                     /* TCP socket file descriptor */
};

int sock_connect (const char *servername, int port);
int sock_sync_data (int sock, int xfer_size, char *local_data, char *remote_data);
int sock_write_data(int sock, int xfer_size, char *local_data);
int sock_read_data(int sock, int max_size, char *local_store);
int poll_completion (struct resources *res);
int post_send (struct resources *res, ibv_wr_opcode opcode);
int post_receive (struct resources *res);

void resources_init (struct resources *res);
int resources_create (struct resources *res, struct config_t &config);
int resources_clone (struct resources *res, struct resources *res_old);
int resources_destroy (struct resources *res);

int connect_qp (struct resources *res, struct config_t &config);
int modify_qp_to_init (struct ibv_qp *qp, struct config_t &config);
int modify_qp_to_rtr (struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid,
        uint8_t * dgid, struct config_t &config);
int modify_qp_to_rts (struct ibv_qp *qp);
