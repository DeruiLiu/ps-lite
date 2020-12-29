// Copyright 2019 Bytedance Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =============================================================================

#ifndef PS_RDMA_TRANSPORT_H_
#define PS_RDMA_TRANSPORT_H_

#ifdef DMLC_USE_RDMA

#include "rdma_utils.h"

namespace ps {

class Transport;

struct Endpoint {
    enum ConnectionStatus { IDLE, CONNECTING, CONNECTED, REJECTED };

    ConnectionStatus status;
    int node_id;
    std::condition_variable cv;
    std::mutex connect_mu;
    struct rdma_cm_id *cm_id;
    struct ibv_ah *ah;  //组播ud时所需
    int remote_qpn;     //组播ud时所需
    int remote_qkey;    //组播ud时所需
    std::string multicast_address;
    std::shared_ptr<Transport> trans;  // Transport作为父类，可以通过多态来访问子类

    int kStartDepth = 128;
    int kRxDepth = 2048;
    int kReplyDepth = kRxDepth;
    WRContext *rx_ctx;
    WRContext *start_ctx;
    WRContext *reply_ctx;

    ThreadsafeQueue<WRContext *> free_start_ctx;
    ThreadsafeQueue<WRContext *> free_reply_ctx;

    Endpoint() : status(IDLE), node_id(Node::kEmpty), cm_id(nullptr), rx_ctx()
    {
        auto byteps_rx_depth = Environment::Get()->find("BYTEPS_RDMA_RX_DEPTH");
        auto byteps_start_depth = Environment::Get()->find("BYTEPS_RDMA_START_DEPTH");
        kStartDepth = byteps_start_depth ? atoi(byteps_start_depth) : kStartDepth;
        kRxDepth = byteps_rx_depth ? atoi(byteps_rx_depth) : kRxDepth;
        kReplyDepth = kRxDepth;

        start_ctx = new WRContext[kStartDepth];
        reply_ctx = new WRContext[kReplyDepth];
        rx_ctx = new WRContext[kRxDepth];
    }

    ~Endpoint()
    {
        for (int i = 0; i < kRxDepth; ++i) {
            if (!(rx_ctx[i].buffer)) {
                continue;
            }
            free(rx_ctx[i].buffer->addr);
            CHECK_EQ(ibv_dereg_mr(rx_ctx[i].buffer), 0);
        }

        for (int i = 0; i < kStartDepth; ++i) {
            if (start_ctx[i].buffer) {
                free(start_ctx[i].buffer->addr);
                CHECK_EQ(ibv_dereg_mr(start_ctx[i].buffer), 0);
            }
        }

        for (int i = 0; i < kReplyDepth; ++i) {
            if (reply_ctx[i].buffer) {
                free(reply_ctx[i].buffer->addr);
                CHECK_EQ(ibv_dereg_mr(reply_ctx[i].buffer), 0);
            }
        }

        rdma_destroy_qp(cm_id);
        CHECK_EQ(rdma_destroy_id(cm_id), 0) << strerror(errno);
    }

    void SetTransport(std::shared_ptr<Transport> t)
    {
        trans = t;
    }

    int GetMultRemoteQpn()
    {
        return remote_qpn;
    }

    int GetMultRemoteQkey()
    {
        return remote_qkey;
    }

    struct ibv_ah *GetMultAh()
    {
        return ah;
    }

    std::shared_ptr<Transport> GetTransport()
    {
        return trans;
    }

    void Disconnect()
    {
        std::unique_lock<std::mutex> lk(connect_mu);
        CHECK_EQ(rdma_disconnect(cm_id), 0) << strerror(errno);
        cv.wait(lk, [this] { return status == IDLE; });
        trans.reset();
    }

    void SetNodeID(int id)
    {
        node_id = id;
    }

    void InitMultSendContextHelper(
        struct ibv_pd *pd, WRContext *ctx, ThreadsafeQueue<WRContext *> *queue, size_t num, WRContextType type)
    {
        for (size_t i = 0; i < num; ++i) {
            void *buf;
            //最大MTU
            aligned_malloc((void **)&buf, 1024);
            CHECK(buf);
            struct ibv_mr *mr = ibv_reg_mr(pd, buf, 1024, 0);
            CHECK(mr) << "ibv_reg_mr failed: " << strerror(errno)
                      << "\nYou can try to reduce BYTEPS_RDMA_START_DEPTH (default 128)"
                      << " or BYTEPS_RDMA_RX_DEPTH (default 2048)";

            ctx[i].type = type;
            ctx[i].buffer = mr;
            ctx[i].private_data = this;  //即this即指该endpoint
            queue->Push(&ctx[i]);
        }
    }

    void InitSendContextHelper(
        struct ibv_pd *pd, WRContext *ctx, ThreadsafeQueue<WRContext *> *queue, size_t num, WRContextType type)
    {
        for (size_t i = 0; i < num; ++i) {
            void *buf;
            aligned_malloc((void **)&buf, kMempoolChunkSize);
            CHECK(buf);
            struct ibv_mr *mr = ibv_reg_mr(pd, buf, kMempoolChunkSize, 0);
            CHECK(mr) << "ibv_reg_mr failed: " << strerror(errno)
                      << "\nYou can try to reduce BYTEPS_RDMA_START_DEPTH (default 128)"
                      << " or BYTEPS_RDMA_RX_DEPTH (default 2048)";

            ctx[i].type = type;
            ctx[i].buffer = mr;
            ctx[i].private_data = this;
            queue->Push(&ctx[i]);
        }
    }

    // worker收到server端的pull response时需要将本地的地址和rkey告知对端，同时提交wr到receive queue
    /*
    void MulticastInitPostRecv(RPRContext &worker_receive){
      char *buffer = worker_receive.buffer;
      int len = worker_receive.len;
      MultWRContext Mwr;
      //Mwr.type = kReceiveContext;
      Mwr.buffer = buffer;
      Mwr.private_data = this;
      Mwr.len = len;
      MultPostRecvWR(Mwr);
    }
    */

    void MulticastInit(struct ibv_cq *cq, struct ibv_pd *pd, int len)
    {
        struct ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(ibv_qp_init_attr));
        attr.send_cq = cq;
        attr.recv_cq = cq;
        attr.cap.max_send_wr = kStartDepth + kReplyDepth;
        attr.cap.max_recv_wr = kRxDepth;
        attr.cap.max_send_sge = kSGEntry;
        attr.cap.max_recv_sge = kSGEntry;
        attr.qp_type = IBV_QPT_UD;
        attr.sq_sig_all = 0;
        int ret = rdma_create_qp(cm_id, pd, &attr);
        if (ret) {
            printf("multicast create qp failed, ret is %d\n", ret);
            return;
        }
        //先不提交wr
        // CHECK_EQ(rdma_create_qp(cm_id, pd, &attr), 0)
        //    << "Create RDMA queue pair failed: " << strerror(errno);
        InitMultSendContextHelper(pd, start_ctx, &free_start_ctx, kStartDepth, kRendezvousStartContext);
        // InitSendContextHelper(pd, start_ctx, &free_start_ctx, kStartDepth, kRendezvousStartContext);
        //发送数据是一块内存
        InitMultSendContextHelper(pd, reply_ctx, &free_reply_ctx, kReplyDepth, kRendezvousReplyContext);
        // InitSendContextHelper(pd, reply_ctx, &free_reply_ctx, kReplyDepth, kRendezvousReplyContext);
        //接收数据是一块内存

        //先塞kRxDepth个wr到接收队列

        for (int i = 0; i < kRxDepth; ++i) {
            void *buf;
            aligned_malloc((void **)&buf, len);
            CHECK(buf);
            struct ibv_mr *mr = ibv_reg_mr(pd, buf, len, IBV_ACCESS_LOCAL_WRITE);
            CHECK(mr) << "ibv_reg_mr failed: " << strerror(errno)
                      << "\nYou can try to reduce BYTEPS_RDMA_START_DEPTH (default 128)"
                      << " or BYTEPS_RDMA_RX_DEPTH (default 2048)";

            rx_ctx[i].type = kReceiveContext;
            rx_ctx[i].buffer = mr;
            rx_ctx[i].private_data = this;
            MulticastPostRecv(&rx_ctx[i]);
        }
    }

    void Init(struct ibv_cq *cq, struct ibv_pd *pd)
    {
        struct ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(ibv_qp_init_attr));
        attr.send_cq = cq;
        attr.recv_cq = cq;
        attr.cap.max_send_wr = kStartDepth + kReplyDepth;
        attr.cap.max_recv_wr = kRxDepth;
        attr.cap.max_send_sge = kSGEntry;
        attr.cap.max_recv_sge = kSGEntry;
        attr.qp_type = IBV_QPT_RC;
        attr.sq_sig_all = 0;

        CHECK_EQ(rdma_create_qp(cm_id, pd, &attr), 0) << "Create RDMA queue pair failed: " << strerror(errno);

        InitSendContextHelper(pd, start_ctx, &free_start_ctx, kStartDepth, kRendezvousStartContext);
        //发送数据时是一块内存
        InitSendContextHelper(pd, reply_ctx, &free_reply_ctx, kReplyDepth, kRendezvousReplyContext);
        //接收数据是一块内存

        for (int i = 0; i < kRxDepth; ++i) {
            void *buf;
            aligned_malloc((void **)&buf, kMempoolChunkSize);
            CHECK(buf);
            struct ibv_mr *mr = ibv_reg_mr(pd, buf, kMempoolChunkSize, IBV_ACCESS_LOCAL_WRITE);
            CHECK(mr) << "ibv_reg_mr failed: " << strerror(errno)
                      << "\nYou can try to reduce BYTEPS_RDMA_START_DEPTH (default 128)"
                      << " or BYTEPS_RDMA_RX_DEPTH (default 2048)";

            rx_ctx[i].type = kReceiveContext;
            rx_ctx[i].buffer = mr;
            rx_ctx[i].private_data = this;

            PostRecv(&rx_ctx[i]);
        }
    }
    void MulticastPostRecv(WRContext *ctx)
    {

        struct ibv_recv_wr wr, *bad_wr = nullptr;
        memset(&wr, 0, sizeof(wr));

        struct ibv_sge sge;
        sge.addr = reinterpret_cast<uint64_t>(ctx->buffer->addr);
        sge.length = 1024;  //最大MTU
        sge.lkey = ctx->buffer->lkey;
        wr.wr_id = reinterpret_cast<uint64_t>(ctx);
        wr.next = nullptr;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        CHECK_EQ(ibv_post_recv(cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_recv failed.";
    }

    void PostRecv(WRContext *ctx)
    {
        struct ibv_recv_wr wr, *bad_wr = nullptr;
        memset(&wr, 0, sizeof(wr));

        struct ibv_sge sge;
        sge.addr = reinterpret_cast<uint64_t>(ctx->buffer->addr);
        sge.length = kMempoolChunkSize;
        sge.lkey = ctx->buffer->lkey;

        wr.wr_id = reinterpret_cast<uint64_t>(ctx);
        wr.next = nullptr;
        wr.sg_list = &sge;
        wr.num_sge = 1;

        CHECK_EQ(ibv_post_recv(cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_recv failed.";
    }
};

class Transport {
public:
    virtual void RDMAWriteWithImm(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) = 0;

    virtual int RecvPushRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
    virtual int RecvPullRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
    virtual int RecvPushResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
    virtual int RecvPullResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;

    virtual void Send(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple) = 0;
    virtual void SendPullRequest(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple) = 0;
    virtual void SendPushRequest(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple) = 0;
    virtual void SendPushResponse(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple) = 0;
    virtual void SendPullResponse(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple, size_t lkey) = 0;
    virtual void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf) = 0;
    virtual void SendRendezvousReply(RendezvousStart *req, AddressPool<BufferContext> &pool) = 0;
    virtual void MultSendMsg(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple, struct ibv_ah *ah,
        int remote_qpn, int remote_rkey) = 0;

    virtual SArray<char> CreateFunctionalSarray(void *value, size_t size) = 0;

};  // class Transport
class RDMATransport : public Transport {
public:
    explicit RDMATransport(Endpoint *endpoint, MemoryAllocator *allocator)
    {
        endpoint_ = CHECK_NOTNULL(endpoint);
        allocator_ = CHECK_NOTNULL(allocator);
        pagesize_ = sysconf(_SC_PAGESIZE);

        auto val = Environment::Get()->find("DMLC_ROLE");
        std::string role(val);
        is_server_ = (role == "server");
    };

    ~RDMATransport(){};

    virtual void RDMAWriteWithImm(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx)
    {
        struct ibv_sge sge;
        sge.addr = reinterpret_cast<uint64_t>(msg_buf->inline_buf);
        sge.length = msg_buf->inline_len;
        sge.lkey = allocator_->LocalKey(msg_buf->inline_buf);

        struct ibv_send_wr wr, *bad_wr = nullptr;
        memset(&wr, 0, sizeof(wr));
        wr.wr_id = reinterpret_cast<uint64_t>(msg_buf);
        wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        wr.next = nullptr;
        wr.imm_data = idx;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey = rkey;

        CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_send failed.";
    }

    void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf)
    {
        WRContext *context = nullptr;
        endpoint_->free_start_ctx.WaitAndPop(&context);  //如果有数据，则进行pop，如果没有数据则进行等待。
        //起初free_start_ctx被塞满

        RendezvousStart *req = reinterpret_cast<RendezvousStart *>(context->buffer->addr);
        req->meta_len = msg_buf->inline_len;
        req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);  //表示本地的地址信息
        req->data_num = msg_buf->data.size();
        for (size_t i = 0; i < req->data_num; ++i) {
            req->data_len[i] = msg.data[i].size();
        }

        struct ibv_sge sge;
        sge.addr = reinterpret_cast<uint64_t>(req);
        sge.lkey = context->buffer->lkey;
        sge.length = sizeof(RendezvousStart);

        struct ibv_send_wr wr, *bad_wr = nullptr;
        memset(&wr, 0, sizeof(wr));
        wr.wr_id = reinterpret_cast<uint64_t>(context);
        wr.opcode = IBV_WR_SEND_WITH_IMM;
        wr.next = nullptr;
        wr.imm_data = kRendezvousStart;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;

        CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0) << strerror(errno);
    }

    //将自己的地址发送给对端
    void SendRendezvousReply(RendezvousStart *req, AddressPool<BufferContext> &addrpool)
    {
        BufferContext *buf_ctx = new BufferContext();
        buf_ctx->meta_len = req->meta_len;
        buf_ctx->data_num = req->data_num;

        size_t data_len = 0;
        for (size_t i = 0; i < req->data_num; ++i) {
            buf_ctx->data_len[i] = req->data_len[i];
            data_len += req->data_len[i];
        }

        // worker only needs a buffer for receving meta
        //分配内存时连这块内存一起注册了，buffer表示注册内存的这块起始地址
        char *buffer =
            allocator_->Alloc(is_server_ ? (align_ceil(req->meta_len, pagesize_) + data_len) : req->meta_len);
        CHECK(buffer);
        buf_ctx->buffer = buffer;

        WRContext *reply_ctx = nullptr;
        endpoint_->free_reply_ctx.WaitAndPop(&reply_ctx);

        RendezvousReply *resp = reinterpret_cast<RendezvousReply *>(reply_ctx->buffer->addr);

        resp->addr = reinterpret_cast<uint64_t>(buffer);  // buffer表示注册的内存的起始地址
        resp->rkey = allocator_->RemoteKey(buffer);
        resp->origin_addr = req->origin_addr;
        resp->idx = addrpool.StoreAddress(buf_ctx);  // buf_ctx即为一个指针

        struct ibv_sge sge;
        sge.addr = reinterpret_cast<uint64_t>(resp);
        sge.length = sizeof(RendezvousReply);
        sge.lkey = reply_ctx->buffer->lkey;

        struct ibv_send_wr wr, *bad_wr = nullptr;
        memset(&wr, 0, sizeof(wr));

        wr.wr_id = reinterpret_cast<uint64_t>(reply_ctx);
        wr.opcode = IBV_WR_SEND_WITH_IMM;
        wr.next = nullptr;
        wr.imm_data = kRendezvousReply;

        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;

        CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_send failed.";
    }

    void Send(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple)
    {
        auto raddr = std::get<0>(remote_tuple);
        auto rkey = std::get<1>(remote_tuple);
        auto idx = std::get<2>(remote_tuple);

        RDMAWriteWithImm(msg_buf, raddr, rkey, idx);
    }

    void SendPushRequest(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple)
    {
        CHECK_EQ(msg_buf->mrs.size(), 1);
        auto raddr = std::get<0>(remote_tuple);
        auto rkey = std::get<1>(remote_tuple);
        auto idx = std::get<2>(remote_tuple);

        // push request, split the meta and data into two writes
        // further, it does not send keys and lens since these meta already carries these info
        struct ibv_sge my_sge;
        my_sge.addr = reinterpret_cast<uint64_t>(msg_buf->mrs[0].first->addr);
        my_sge.length = msg_buf->mrs[0].second;
        my_sge.lkey = msg_buf->mrs[0].first->lkey;

        // this rdma-write will not trigger any signal both remotely and locally
        struct ibv_send_wr wr, *bad_wr = nullptr;
        memset(&wr, 0, sizeof(wr));
        wr.wr_id = 0;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.next = nullptr;
        wr.sg_list = &my_sge;
        wr.num_sge = 1;
        wr.wr.rdma.rkey = rkey;

        // write to the next page-aligned address (remote_addr should already be aligned)
        wr.wr.rdma.remote_addr = raddr + align_ceil(msg_buf->inline_len, pagesize_);

        CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_send failed.";

        RDMAWriteWithImm(msg_buf, raddr, rkey, idx);
    }

    void SendPullRequest(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple)
    {
        CHECK_EQ(msg_buf->mrs.size(), 0);
        Send(msg, msg_buf, remote_tuple);
    }

    virtual void SendPushResponse(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple)
    {
        CHECK_EQ(msg_buf->mrs.size(), 0);
        Send(msg, msg_buf, remote_tuple);
    }

    //对应组播发送数据
    void MultSendMsg(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple, struct ibv_ah *ah, int remote_qpn,
        int remote_rkey)
    {
        auto len = msg.meta.val_len;
        auto addr = msg_buf->data[1].data();
        int data_length = 0;
        auto raddr = std::get<0>(remote_tuple);
        // auto idx = std::get<2>(remote_tuple);
        while (data_length < len) {
            LOG(INFO)<<"data_length= "<<data_length;
            // data_length表示本地拷贝了多少数据，len表示一共多少数据，addr表示对端接收到数据后需要把该数据拷贝到addr所在的地址
            WRContext *context = nullptr;
            endpoint_->free_start_ctx.WaitAndPop(&context);
            //LOG(INFO)<<"222222222222222222222222222222";
            MultMsgMeta *MultMeta = reinterpret_cast<MultMsgMeta *>(context->buffer->addr);
            // MultMeta->idx = idx;
            MultMeta->addr = raddr;
            //此处1024表示最大MTU，用1024代替
            int payload = 1024 - sizeof(MultMeta->addr) + sizeof(MultMeta->len);
            int length = data_length + payload > len ? len - data_length : payload;
            MultMeta->len = length;
            data_length += length;
            addr += length;
            raddr += length;
            //LOG(INFO)<<"addr= "<<addr<<" MultMeta->data="<<MultMeta->data<<" len="<<MultMeta->len;
            memcpy(addr, MultMeta + sizeof(MultMeta->addr) + sizeof(MultMeta->len), MultMeta->len);  //将数据拷贝到wr所在内存发送，因为发送时需要拼接，所以需要拷贝
            struct ibv_sge sge;
            sge.addr = reinterpret_cast<uint64_t>(MultMeta);
            sge.lkey = context->buffer->lkey;
            sge.length = length + sizeof(MultMeta->addr) + sizeof(MultMeta->len);
            //LOG(INFO)<<"44444444444444444444444444444444";
            struct ibv_send_wr wr, *bad_wr = nullptr;
            memset(&wr, 0, sizeof(wr));
            wr.wr_id = reinterpret_cast<uint64_t>(context);
            wr.opcode = IBV_WR_SEND_WITH_IMM;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.ud.ah = ah;
            wr.wr.ud.remote_qpn = remote_qpn;
            wr.wr.ud.remote_qkey = remote_rkey;
            wr.next = NULL;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_send failed.";
        }
        LOG(INFO) << "use multicast to send msg , remote_qpn=" << remote_qpn;
        // after write keys/vals/lens (no imm), write the meta (with imm)
        Send(msg, msg_buf, remote_tuple);
    }

    virtual void SendPullResponse(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple, size_t lkey)
    {

        /*
        // multicast发送数据时需要按照指定结构体切分，并选择对应的组播QP发送
        int send_id = msg.meta.sender;
        m_endpoints_mu.lock();
        auto it = m_endpoints_.find(send_id);
        if (it == m_endpoints_.end()) {
            LOG(INFO) << "cann't not post wr to the receive queue, because can't find qp connection";
            m_endpoints_mu.unlock();
        } else {
            LOG(INFO) << "i find qp connection";
            Endpoint *endpoint = m_endpoints_[send_id].get();
            m_endpoints_mu_.unlock();
            auto trans = CHECK_NOTNULL(endpoint->GetTransport());
            auto ah = endpoint->GetMultAh();
            auto remote_qpn = endpoint->GetMultRemoteQpn();
            auto remote_qkey = endpoint->GetMultRemoteQkey();
            trans->MultSendMsg(msg, msg_buf, remote_tuple, ah, remote_qpn, remote_qkey);
        }
        */

        //此处为原始情况，改为组播

        CHECK_EQ(msg_buf->mrs.size(), 0);

        auto raddr = msg.meta.addr;
        auto rkey = msg.meta.option;
        auto len = msg.meta.val_len;
        CHECK_EQ((size_t)msg.meta.val_len, msg_buf->data[1].size());

        struct ibv_sge sge;
        sge.addr = reinterpret_cast<uint64_t>(msg_buf->data[1].data());
        sge.length = len;
        sge.lkey = lkey;

        // this rdma-write will not trigger any signal both remotely and locally
        struct ibv_send_wr wr, *bad_wr = nullptr;
        memset(&wr, 0, sizeof(wr));
        wr.wr_id = reinterpret_cast<uint64_t>(raddr);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.next = nullptr;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = raddr;
        wr.wr.rdma.rkey = rkey;

        CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_send failed.";

        // after write keys/vals/lens (no imm), write the meta (with imm)
        Send(msg, msg_buf, remote_tuple);
    }

    virtual int RecvPushResponse(Message *msg, BufferContext *buffer_ctx, int meta_len)
    {
        CHECK_EQ(buffer_ctx->data_num, 0);
        return 0;
    }

    virtual int RecvPullRequest(Message *msg, BufferContext *buffer_ctx, int meta_len)
    {
        SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));

        SArray<char> vals;  // add an empty sarray to pass kvapp check

        msg->data.push_back(keys);
        msg->data.push_back(vals);

        return keys.size() + vals.size();
    }

    virtual int RecvPushRequest(Message *msg, BufferContext *buffer_ctx, int meta_len)
    {
        CHECK(msg->meta.push && msg->meta.request);
        CHECK_EQ(buffer_ctx->data_num, 3);
        uint32_t len = buffer_ctx->data_len[1];
        char *cur = buffer_ctx->buffer + align_ceil((size_t)meta_len, pagesize_);

        SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));

        SArray<char> vals;
        vals.reset(cur, len, [](void *) {});  // no need to delete

        SArray<char> lens = CreateFunctionalSarray(&msg->meta.val_len, sizeof(int));

        msg->data.push_back(keys);
        msg->data.push_back(vals);
        msg->data.push_back(lens);

        return keys.size() + vals.size() + lens.size();
    }

    virtual int RecvPullResponse(Message *msg, BufferContext *buffer_ctx, int meta_len)
    {
        SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));

        SArray<char> vals;
        auto addr = msg->meta.addr;
        vals.reset(reinterpret_cast<char *>(addr), msg->meta.val_len, [](void *) {});

        SArray<char> lens = CreateFunctionalSarray(&msg->meta.val_len, sizeof(int));

        msg->data.push_back(keys);
        msg->data.push_back(vals);
        msg->data.push_back(lens);

        return keys.size() + vals.size() + lens.size();
    }

    SArray<char> CreateFunctionalSarray(void *value, size_t size)
    {
        SArray<char> sarr;
        void *p = malloc(size);
        memcpy(p, value, size);
        sarr.reset((char *)p, size, [p](void *) { free(p); });
        return sarr;
    }

protected:
    size_t pagesize_ = 4096;
    Endpoint *endpoint_;
    MemoryAllocator *allocator_;
    bool is_server_;

};  // class Transport

class IPCTransport : public RDMATransport {
public:
    explicit IPCTransport(Endpoint *endpoint, MemoryAllocator *allocator) : RDMATransport(endpoint, allocator)
    {
        auto val = Environment::Get()->find("BYTEPS_IPC_COPY_NUM_THREADS");
        ipc_copy_nthreads_ = val ? atoi(val) : 4;
        for (int i = 0; i < ipc_copy_nthreads_; ++i) {
            auto q = new ThreadsafeQueue<AsyncCopy>;
            async_copy_queue_.push_back(q);
        }
        for (int i = 0; i < ipc_copy_nthreads_; ++i) {
            auto t = new std::thread(&IPCTransport::AsyncCopyThread, this, i);
            ipc_copy_thread_list_.push_back(t);
        }
        val = Environment::Get()->find("BYTEPS_PARTITION_BYTES");
        byteps_partition_bytes_ = val ? atoi(val) : 4096000;

        val = Environment::Get()->find("BYTEPS_LOCAL_SIZE");
        auto byteps_local_size = val ? atoi(val) : 8;
        byteps_partition_bytes_ = RoundUp(byteps_partition_bytes_, byteps_local_size * sysconf(_SC_PAGESIZE));

        val = Environment::Get()->find("BYTEPS_IPC_ENABLE_ASYNC_COPY");
        enable_async_copy_ = val ? atoi(val) : 1;  // default enabled
        if (!enable_async_copy_)
            LOG(INFO) << "Async copy has been disabled, this could affect the performance";

        val = Environment::Get()->find("BYTEPS_PCIE_SWITCH_SIZE");
        auto byteps_nccl_pcie_size = val ? atoi(val) : 8;
        if (byteps_local_size % byteps_nccl_pcie_size != 0) {
            // local_size < pcie_size or unbalance PCIe switches
            byteps_nccl_pcie_size = byteps_local_size;
        }
        // ensure this name corresponds with that in BytePSSharedMemory::openPcieSharedMemory()
        if (byteps_local_size > byteps_nccl_pcie_size) {
            // cross pcie switch, use the last pcie cpu buffer
            auto byteps_pcie_num = byteps_local_size / byteps_nccl_pcie_size;
            shm_prefix_ = kShmPciePrefix + std::to_string(byteps_pcie_num - 1) + "_Shm_";
        } else {
            shm_prefix_ = kShmPrefix;
        }
    };

    ~IPCTransport()
    {
        for (size_t i = 0; i < ipc_copy_thread_list_.size(); ++i) {
            AsyncCopy m;
            m.shutdown = true;
            async_copy_queue_[i]->Push(m);
            ipc_copy_thread_list_[i]->join();
        }
    }

    void SendPushRequest(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple)
    {
        Send(msg, msg_buf, remote_tuple);
    }

    void SendPullResponse(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple, size_t lkey)
    {
        auto addr = (void *)CHECK_NOTNULL(msg.data[1].data());
        void *shm_addr = CHECK_NOTNULL(GetSharedMemory(shm_prefix_, msg.meta.key));

        if (enable_async_copy_) {
            // async copy with a simple load-balancing strategy
            AsyncCopy m = {msg_buf, remote_tuple, shm_addr, addr, msg.meta.val_len, false};
            auto cnt = cpy_counter_.fetch_add(1);
            async_copy_queue_[cnt % ipc_copy_nthreads_]->Push(m);
        } else {
            // synchronous copy
            memcpy(shm_addr, addr, msg.meta.val_len);
            Send(msg, msg_buf, remote_tuple);
        }
    }

    int RecvPushRequest(Message *msg, BufferContext *buffer_ctx, int meta_len)
    {
        // get data message from local shared memory
        auto key = msg->meta.key;
        auto len = msg->meta.val_len;

        SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));

        SArray<char> vals;
        void *addr = GetSharedMemory(shm_prefix_, key);
        vals.reset(reinterpret_cast<char *>(addr), len, [](void *) {});

        SArray<char> lens = CreateFunctionalSarray(&msg->meta.val_len, sizeof(int));

        msg->data.push_back(keys);
        msg->data.push_back(vals);
        msg->data.push_back(lens);

        return keys.size() + vals.size() + lens.size();
    }

private:
    struct AsyncCopy {
        MessageBuffer *msg_buf;
        RemoteTuple remote_tuple;
        void *dst;
        void *src;
        int len;
        bool shutdown;
    };

    void AsyncCopyThread(int i)
    {
        auto &q = async_copy_queue_[i];
        while (true) {
            AsyncCopy m;
            q->WaitAndPop(&m);
            if (m.shutdown)
                break;
            if (m.len == 0)
                continue;

            // TODO: use parallel copy
            CHECK(m.dst);
            CHECK(m.src);
            memcpy(m.dst, m.src, m.len);

            auto raddr = std::get<0>(m.remote_tuple);
            auto rkey = std::get<1>(m.remote_tuple);
            auto idx = std::get<2>(m.remote_tuple);

            RDMAWriteWithImm(m.msg_buf, raddr, rkey, idx);
        }
    }

    void *GetSharedMemory(const std::string &prefix, uint64_t key)
    {
        std::lock_guard<std::mutex> lock(shm_mu_);
        auto worker_key = DecodeWorkerKey(key);
        auto seq_num = worker_key % (1 << 16);
        auto base_key = worker_key - seq_num;
        uint64_t offset = byteps_partition_bytes_ * seq_num;
        if (key_shm_addr_.find(base_key) != key_shm_addr_.end()) {
            return (void *)((char *)key_shm_addr_[base_key] + offset);
        }
        std::string shm_name(prefix);
        shm_name += std::to_string(base_key);
        int shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0666);
        CHECK_GE(shm_fd, 0) << "shm_open failed for " << shm_name << ", " << strerror(errno);

        struct stat sb;
        CHECK_EQ(0, fstat(shm_fd, &sb)) << strerror(errno);
        auto total_shm_size = sb.st_size;

        void *base_ptr = mmap(0, total_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        CHECK_NE(base_ptr, (void *)-1) << strerror(errno);
        key_shm_addr_[base_key] = base_ptr;

        PS_VLOG(1) << "open Shared Memory: " << shm_name << ", offset=" << offset
                   << ", (in bytes) size=" << total_shm_size;
        return (void *)((char *)key_shm_addr_[base_key] + offset);
    }

    int ipc_copy_nthreads_;
    std::vector<std::thread *> ipc_copy_thread_list_;
    std::vector<ThreadsafeQueue<AsyncCopy> *> async_copy_queue_;
    std::atomic<unsigned long long> cpy_counter_{0};

    int byteps_partition_bytes_ = 4096000;

    std::string shm_prefix_;

    std::mutex shm_mu_;
    std::unordered_map<uint64_t, void *> key_shm_addr_;

    bool enable_async_copy_;

};  // class IPCTransport

};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_
