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

#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_

#ifdef DMLC_USE_RDMA

#include "rdma_utils.h"
#include "rdma_transport.h"

namespace ps {

class RDMAVan : public Van {
public:
    RDMAVan()
    {
        CHECK_EQ(ibv_fork_init(), 0) << strerror(errno);
    }
    ~RDMAVan()
    {}

    virtual std::string GetType() const
    {
        return std::string("rdma");
    }

protected:
    void Start(int customer_id, bool standalone) override
    {
        start_mu_.lock();
        should_stop_ = false;

        auto val = Environment::Get()->find("BYTEPS_ENABLE_IPC");  //启用进程间通信，能够加快训练速度
        disable_ipc_ = val ? !atoi(val) : true;
        if (disable_ipc_)
            LOG(INFO) << "Shared memory IPC has been disabled";

        if (event_channel_ == nullptr) {
            event_channel_ =
                rdma_create_event_channel();  //创建一个用于接收events的通道,刚开始创建该通道是为了向schedule发送本地节点信息
            CHECK(event_channel_) << "Create RDMA event channel failed";

            //启用一个本地线程来持续监听收到的events
            cm_event_polling_thread_.reset(new std::thread(&RDMAVan::PollEvents, this));
        }

        // enable logging
        val = Environment::Get()->find("BYTEPS_PRINT_RDMA_LOG");
        enable_log_ = val ? atoi(val) : false;
        if (enable_log_)
            LOG(INFO) << "Enable RDMA logging.";

        val = Environment::Get()->find("BYTEPS_RDMA_MAX_CONCURR_WR");  //即环境变量中是否引入了这些参数的设置
        if (val) {
            // should make sure: kMaxConcurrentWorkRequest >= kStartDepth + kReplyDepth + kRxDepth
            kMaxConcurrentWorkRequest = atoi(val);

            auto start_depth_env = Environment::Get()->find("BYTEPS_RDMA_START_DEPTH");
            auto rx_depth_env = Environment::Get()->find("BYTEPS_RDMA_RX_DEPTH");

            auto start_depth = start_depth_env ? atoi(start_depth_env) : 128;
            auto rx_depth = rx_depth_env ? atoi(rx_depth_env) : 2048;
            auto reply_depth = rx_depth;

            CHECK_GE(kMaxConcurrentWorkRequest, start_depth + reply_depth + rx_depth)
                << "Should make sure: kMaxConcurrentWorkRequest >= kStartDepth + kReplyDepth + kRxDepth";
        }

        start_mu_.unlock();

        // Postoffice传入的standalone为flase，即会再次调用一次Van::Start()
        if (!standalone)
            Van::Start(customer_id, false);  //为什么此处又要去调用Van对应的Start？因为需要使得当前节点与Scheduler相连
    }

    void Stop() override
    {
        LOG(INFO) << " now is stop      ";
        if (my_node_.role != Node::SCHEDULER) {
            LOG(INFO) << "leave multicast first";
            for (auto &c : m_record) {
                struct addrinfo *remote_addr;
                CHECK_EQ(getaddrinfo(c.addr.c_str(), nullptr, nullptr, &remote_addr), 0);
                rdma_leave_multicast(c.cm_id, remote_addr->ai_addr);
            }

            PS_VLOG(1) << "Stopping m_cq_polling_thread_.";
            m_cq_polling_thread_->join();
            PS_VLOG(1) << "m_cq_polling_thread_->join completion";
            m_cq_polling_thread_.reset();
            PS_VLOG(1) << "m_cq_polling_thread_.reset() completion";

            PS_VLOG(1) << "Clearing memory multicast allocator.";
            m_mem_allocator_.reset();

            PS_VLOG(1) << "Clearing multicast endpoints.";
            {
                std::lock_guard<std::mutex> lk(m_endpoints_mu_);
                m_endpoints_.clear();
            }

            PS_VLOG(1) << "Destroying multicast cq and pd.";
            CHECK(!ibv_destroy_cq(m_cq_)) << "Failed to destroy CQ";
            CHECK(!ibv_destroy_comp_channel(m_comp_event_channel_)) << "Failed to destroy channel";
        }

        LOG(INFO) << "  scheduler begin to stop ";
        PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
        Van::Stop();

        should_stop_ = true;
        CHECK(should_stop_);

        PS_VLOG(1) << "Stopping cq_polling_thread_.";
        cq_polling_thread_->join();
        cq_polling_thread_.reset();

        PS_VLOG(1) << "Stopping cm_event_polling_thread_.";
        cm_event_polling_thread_->join();
        cm_event_polling_thread_.reset();

        PS_VLOG(1) << "Clearing memory allocator.";
        mem_allocator_.reset();

        PS_VLOG(1) << "Clearing endpoints.";
        incoming_.clear();
        {
            std::lock_guard<std::mutex> lk(endpoints_mu_);
            endpoints_.clear();
        }

        PS_VLOG(1) << "Destroying cq and pd.";
        CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
        CHECK(!ibv_destroy_comp_channel(comp_event_channel_)) << "Failed to destroy channel";

        for (auto &it : mem_mr_)
            ibv_dereg_mr(it.second);

        // TODO: ibv_dealloc_pd sometimes complains resource busy, need to fix this
        // CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD: " <<
        // strerror(errno);

        PS_VLOG(1) << "Destroying listener.";
        rdma_destroy_id(listener_);
        rdma_destroy_event_channel(event_channel_);
    }

    int Bind(const Node &node, int max_retry) override
    {
        CHECK(rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) ==
              0)  //创建用于跟踪通信信息的标识符，概念上类似于套接字
            << "Create RDMA connection identifier failed";

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));

        auto val = Environment::Get()->find("DMLC_NODE_HOST");
        if (val) {
            PS_VLOG(1) << "bind to DMLC_NODE_HOST: " << std::string(val);
            addr.sin_addr.s_addr = inet_addr(val);
        }

        addr.sin_family = AF_INET;
        int port = node.port;
        unsigned seed = static_cast<unsigned>(time(NULL) + port);
        for (int i = 0; i < max_retry + 1; ++i) {
            addr.sin_port = htons(port);
            if (rdma_bind_addr(listener_,  //将本地地址与rdma_cm_id关联起来
                    reinterpret_cast<struct sockaddr *>(&addr)) == 0) {
                break;
            }
            if (i == max_retry) {
                port = -1;
            } else {
                port = 10000 + rand_r(&seed) % 40000;
            }
        }
        //启动侦听传入的连接请求，监听本地绑定的源地址
        CHECK(rdma_listen(listener_, kRdmaListenBacklog) == 0) << "Listen RDMA connection failed: " << strerror(errno);
        return port;
    }

    //自己添加的组播相关代码，如果自己是server，则连接自己绑定的组播地址；如果自己自己是worker，则连接server绑定的组播地址
    void M_Connect(const Node &node) override
    {
        // LOG(INFO) <<"M_connect is multicast "<< "Connecting to Node " << node.id << ", My_node=" << my_node_.id;
        PS_VLOG(1) << "Connecting to Node " << node.id << ", My_node=" << my_node_.id;
        CHECK_NE(node.id, node.kEmpty);
        CHECK_NE(node.port, node.kEmpty);
        CHECK(node.hostname.size());

        // worker doesn't need to connect to the other workers. same for server
        if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
            return;
        }

        if (node.id != Node::kEmpty) {
            Endpoint *endpoint;
            struct addrinfo *remote_addr;
            std::string multicast_address;

            //如果是server的话，则组播时绑定自己的ID信息,一个组播地址只能用于一个组

            // my_node_.role == SERVER
            // LOG(INFO) <<"my id = "<<my_node_.id<<" node id = "<<node.id;
            // if(node.role != Node::WORKER) return;
            if (my_node_.role == Node::SERVER) {  // server
                m_endpoints_mu_.lock();
                // if(node.role != Node::WORKER) return;
                auto it = m_endpoints_.find(my_node_.id);
                // if there is an endpoint with pending connection
                if (it != m_endpoints_.end()) {
                    // LOG(INFO) << " i find i have connected multicast my_node_.id = " << my_node_.id;
                    m_endpoints_mu_.unlock();
                    return;
                }

                LOG(INFO) << " i find i have connected multicast my_node_.id = " << my_node_.id;

                m_endpoints_[my_node_.id] = std::make_unique<Endpoint>();  // m_endpoints_表示本地启用的组播连接
                endpoint = m_endpoints_[my_node_.id].get();
                m_endpoints_mu_.unlock();

                endpoint->SetNodeID(my_node_.id);
                // 224.224.1.2作为起始地址
                multicast_address = "224.224.1.";
                int tail = (2 + my_node_.id) % 255;
                multicast_address += std::to_string(tail);
                // MulticastInfo mm;
                // mm.addr = multicast_address;
                // mm.used = false;
                // m_record.push_back(mm);  //将本地要连接的组播地址记录下来，方便在event的时候join
                CHECK_EQ(getaddrinfo(multicast_address.c_str(), nullptr, nullptr, &remote_addr), 0);
                // LOG(INFO) << "Connect to Node " << my_node_.id << " with multicast, my role is server";

            } else if (my_node_.role == Node::WORKER) {  //如果是worker的话，则组播时绑定server的id信息
                if (node.role != Node::SERVER)
                    return;
                m_endpoints_mu_.lock();
                auto it = m_endpoints_.find(node.id);
                // if there is an endpoint with pending connection
                if (it != m_endpoints_.end()) {
                    m_endpoints_mu_.unlock();
                    return;
                }
                m_endpoints_[node.id] = std::make_unique<Endpoint>();  // m_endpoints_表示本地启用的连接对端的rdma连接
                endpoint = m_endpoints_[node.id].get();
                m_endpoints_mu_.unlock();
                endpoint->SetNodeID(node.id);
                // 224.224.1.2作为起始地址
                multicast_address = "224.224.1.";
                int tail = (2 + node.id) % 255;
                multicast_address += std::to_string(tail);
                // struct MulticastInfo mm;
                // mm.addr = multicast_address;
                // mm.used = false;
                // m_record.push_back(mm);  //将本地要连接的组播地址记录下来，方便在event的时候join
                CHECK_EQ(getaddrinfo(multicast_address.c_str(), nullptr, nullptr, &remote_addr), 0);
                // LOG(INFO) << "Connect to Node " << node.id << " with multicast, my role is worker";
            }

            while (endpoint->status != Endpoint::CONNECTED) {
                std::unique_lock<std::mutex> lk(endpoint->connect_mu);
                endpoint->status = Endpoint::CONNECTING;

                if (endpoint->cm_id != nullptr) {
                    rdma_destroy_qp(endpoint->cm_id);
                    CHECK_EQ(rdma_destroy_id(endpoint->cm_id), 0) << strerror(errno);
                    endpoint->cm_id = nullptr;
                }
                //分配一个rdma_cm_id，与socket相似
                CHECK_EQ(rdma_create_id(event_channel_, &endpoint->cm_id, nullptr, RDMA_PS_UDP), 0)
                    << "Create RDMA connection identifier failed";
                endpoint->cm_id->context = endpoint;  //此处的context应该指buf，需要分配内存空间
                MulticastInfo mm;
                mm.addr = multicast_address;
                mm.used = false;
                mm.cm_id = endpoint->cm_id;
                m_record.push_back(mm);  //将本地要连接的组播地址记录下来，方便在event的时候join

                listen_id_list.push_back(endpoint->cm_id);  //将UDP的listen_id记录下来，listen_id_list即表示UDP的id

                auto val = Environment::Get()->find(
                    "DMLC_NODE_HOST_MULTICAST");  //表示自己的地址,为了不与原始ps-lite冲突，故更名为DMLC_NODE_HOST_MULTICAST
                if (val) {
                    struct addrinfo *addr;
                    auto rc = getaddrinfo(val, "", NULL, &addr);
                    CHECK_EQ(rc, 0) << "getaddrinfo failed: " << gai_strerror(rc);
                    CHECK_EQ(rdma_bind_addr(endpoint->cm_id, addr->ai_addr), 0)
                        << "multicast bind addr error: " << strerror(errno);

                    CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, addr->ai_addr, remote_addr->ai_addr, kTimeoutms), 0)
                        << "Resolve RDMA address failed with errno: " << strerror(errno);
                    // LOG(INFO) << " use my host address " << val << " ";
                } else {  //获得一个本地设备来连接到远程地址
                    struct sockaddr_in addr;
                    memset(&addr, 0, sizeof(addr));
                    addr.sin_family = AF_INET;
                    int port = 20077;
                    unsigned seed = static_cast<unsigned>(time(NULL) + port);
                    for (int i = 0; i < 40 + 1; ++i) {
                        addr.sin_port = htons(port);
                        if (rdma_bind_addr(endpoint->cm_id,  //将本地地址与rdma_cm_id关联起来
                                reinterpret_cast<struct sockaddr *>(&addr)) == 0) {
                            break;
                        }
                        if (i == 40) {
                            port = -1;
                        } else {
                            port = 10000 + rand_r(&seed) % 40000;
                        }
                    }

                    // CHECK_EQ(rdma_bind_addr(endpoint->cm_id, (struct sockaddr *)(&addr)),0) << "multicast bind addr
                    // error: "<<strerror(errno);
                    CHECK_EQ(rdma_resolve_addr(endpoint->cm_id,
                                 reinterpret_cast<struct sockaddr *>(&addr),
                                 remote_addr->ai_addr,
                                 kTimeoutms),
                        0)
                        << "Resolve RDMA address failed with errno: " << strerror(errno);
                }
                // LOG(INFO) << "multicast address is " << multicast_address << " remote_addr is " <<
                // remote_addr->ai_addr;
                endpoint->cv.wait(lk, [endpoint] { return endpoint->status != Endpoint::CONNECTING; });

                if (endpoint->status == Endpoint::CONNECTED)
                    break;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            bool mm_role = (my_node_.role == Node::WORKER);
            LOG(INFO) << "Connect to Node " << multicast_address
                      << " with multicast, my role = " << (mm_role ? "worker" : "server");
            // LOG(INFO) << "Connect to Node " << node.id << " with Transport=" << (is_local_node ? "IPC" : "RDMA");
            // mem_allocator_为RDMAVan的内存分配器，make_shared在动态内存中分配一个对象并初始化它，返回指向此对象的shared_ptr
            std::shared_ptr<Transport> t = std::make_shared<RDMATransport>(endpoint, m_mem_allocator_.get());
            endpoint->SetTransport(t);
            freeaddrinfo(remote_addr);
        }
    }

    void Connect(const Node &node) override
    {  //表示本端去连接node这个节点
        PS_VLOG(1) << "Connecting to Node " << node.id << ", My_Node=" << my_node_.id;
        CHECK_NE(node.id, node.kEmpty);
        CHECK_NE(node.port, node.kEmpty);
        CHECK(node.hostname.size());

        // worker doesn't need to connect to the other workers. same for server
        if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
            return;
        }

        if (node.id != Node::kEmpty) {
            endpoints_mu_.lock();
            auto it = endpoints_.find(node.id);  // endpoints_保持RDMA中的连接信息，与node相应节点的连接信息

            // if there is an endpoint with pending connection，如果本端已经与这个node.id有rdma连接，则先删除
            if (it != endpoints_.end()) {
                endpoints_.erase(it);
            }

            Endpoint *endpoint;
            endpoints_[node.id] = std::make_unique<Endpoint>();  // endpoints_表示本地启用的连接对端的rdma连接
            endpoint = endpoints_[node.id].get();
            endpoints_mu_.unlock();

            endpoint->SetNodeID(node.id);

            struct addrinfo *remote_addr;
            CHECK_EQ(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(), nullptr, &remote_addr), 0);

            //接下来的一套流程即为CM下RDMA建链的一套流程,此处还未分client端还是server端
            while (endpoint->status != Endpoint::CONNECTED) {
                std::unique_lock<std::mutex> lk(endpoint->connect_mu);
                endpoint->status = Endpoint::CONNECTING;

                if (endpoint->cm_id != nullptr) {
                    rdma_destroy_qp(endpoint->cm_id);
                    CHECK_EQ(rdma_destroy_id(endpoint->cm_id), 0) << strerror(errno);
                    endpoint->cm_id = nullptr;
                }

                //分配一个rdma_cm_id，与socket相似
                CHECK_EQ(rdma_create_id(event_channel_, &endpoint->cm_id, nullptr, RDMA_PS_TCP), 0)
                    << "Create RDMA connection identifier failed";
                endpoint->cm_id->context = endpoint;  //此处的context应该指buf，需要分配内存空间

                auto val = Environment::Get()->find("DMLC_NODE_HOST");
                if (val) {
                    struct addrinfo *addr;
                    auto rc = getaddrinfo(val, "", NULL, &addr);
                    CHECK_EQ(rc, 0) << "getaddrinfo failed: " << gai_strerror(rc);

                    CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, addr->ai_addr, remote_addr->ai_addr, kTimeoutms), 0)
                        << "Resolve RDMA address failed with errno: " << strerror(errno);
                } else {  //获得一个本地设备来连接到远程地址
                    CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, nullptr, remote_addr->ai_addr, kTimeoutms), 0)
                        << "Resolve RDMA address failed with errno: " << strerror(errno);
                }

                endpoint->cv.wait(lk, [endpoint] { return endpoint->status != Endpoint::CONNECTING; });

                if (endpoint->status == Endpoint::CONNECTED)
                    break;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            bool is_local_node = disable_ipc_ ? false : (node.hostname == my_node_.hostname ? true : false);
            {
                std::lock_guard<std::mutex> lk(local_mu_);
                is_local_[node.id] = is_local_node;
            }

            LOG(INFO) << "Connect to Node " << node.id << " with Transport=" << (is_local_node ? "IPC" : "RDMA");
            // mem_allocator_为RDMAVan的内存分配器，make_shared在动态内存中分配一个对象并初始化它，返回指向此对象的shared_ptr
            std::shared_ptr<Transport> t = is_local_node
                                               ? std::make_shared<IPCTransport>(endpoint, mem_allocator_.get())
                                               : std::make_shared<RDMATransport>(endpoint, mem_allocator_.get());
            endpoint->SetTransport(t);

            freeaddrinfo(remote_addr);
        }
    }

    //初始化环境后，最开始先由worker发起一个SendMsg，里面包含相关信息
    int SendMsg(Message &msg) override
    {
        int remote_id = msg.meta.recver;  //这个msg的接收端
        CHECK_NE(remote_id, Meta::kEmpty);

        endpoints_mu_.lock();
        CHECK_NE(endpoints_.find(remote_id), endpoints_.end());  // endpoints_表示对端的server节点组成的集合
        Endpoint *endpoint = endpoints_[remote_id].get();        //表示和remote_id匹配的rdma连接,取出该连接
        endpoints_mu_.unlock();

        int meta_len = GetPackMetaLen(msg.meta);  //消息元数据长度
        size_t data_len = msg.meta.data_size;     //消息数据体长度，数据是由类似vector组成
        size_t total_len = meta_len + data_len;   //消息总长度
        CHECK(meta_len);

        RegisterMemory(msg);  //为msg中所含数据块中的vals注册内存

        // pack meta info
        if (IsValidPushpull(msg)) {  //若控制信息为空，则将本地地址的rkey加入到meta中
            AddMeta(msg);
        }

        auto trans = CHECK_NOTNULL(endpoint->GetTransport());

        // start rendezvous if no remote info
        if (!IsValidPushpull(msg)) {                         //控制信息不为空，
            MessageBuffer *msg_buf = PrepareNewMsgBuf(msg);  //为这块msg的meta注册一块内存用来发送数据
            StoreMsgBuf(msg_buf, msg);                       //将msg_buf和msg.meta对应关系存储下来
            trans->SendRendezvousBegin(msg, msg_buf);  //将这个请求信息发送，但是发送时并没有携带本端的address和rkey
            return total_len;

        } else {  //控制信息为空，则表示传数据？
            auto is_push = msg.meta.push;
            auto key = msg.meta.key;

            if (!HasRemoteInfo(msg, key, is_push, remote_id)) {  //这个时候如果没有远端的地址信息
                MessageBuffer *msg_buf = PrepareNewMsgBuf(msg);  //为这块msg注册一块内存用来发送
                StoreMsgBuf(msg_buf, msg);                       //将msg_buf和msg的对应关系存储下来
                PrepareData(msg, msg_buf);
                trans->SendRendezvousBegin(msg, msg_buf);  //将这个请求信息发送，但是发送时并没有携带本端的address和rkey
                return total_len;
            }
        }

        auto addr_tuple = GetRemoteAndLocalInfo(msg.meta.key, msg.meta.push, remote_id);
        MessageBuffer *msg_buf = std::get<3>(addr_tuple);  // local message buffer

        // prepare new meta and data
        CHECK_EQ(msg_buf->inline_len, (size_t)meta_len);
        CHECK(msg_buf->inline_buf);
        msg_buf->data = msg.data;  // may not need this
        PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

        PrintSendLog(msg, msg_buf, addr_tuple);

        //表示自己要发送的数据类型
        // already know remote address, directly use RDMA-write
        if (msg.meta.push && msg.meta.request) {
            // worker, push request，自己要发送一个push request的请求
            trans->SendPushRequest(msg, msg_buf, addr_tuple);  //将meta和data(keys,vals,lens)分开写
        } else if (msg.meta.push && !msg.meta.request) {
            // server, push response
            trans->SendPushResponse(msg, msg_buf, addr_tuple);
        } else if (!msg.meta.push && msg.meta.request) {
            // worker, pull request
            trans->SendPullRequest(msg, msg_buf, addr_tuple);
        } else if (!msg.meta.push && !msg.meta.request) {
            // server, pull response
            map_mu_.lock();
            auto temp_mr = mem_mr_.find(msg_buf->data[1].data());
            CHECK_NE(temp_mr, mem_mr_.end());
            map_mu_.unlock();
            trans->SendPullResponse(msg, msg_buf, addr_tuple, temp_mr->second->lkey);
        } else {
            CHECK(0) << "unexpected message type";
        }

        return total_len;
    }

    int RecvMsg(Message *msg) override
    {
        msg->data.clear();
        std::tuple<Endpoint *, BufferContext *> notification;
        recv_buffers_.WaitAndPop(&notification);  //该调用是阻塞的

        Endpoint *endpoint = std::get<Endpoint *>(notification);
        BufferContext *buffer_ctx = std::get<BufferContext *>(notification);

        msg->meta.recver = my_node_.id;        //表示接收端为自己
        msg->meta.sender = endpoint->node_id;  //发送端为对端

        // the second argument is actually deprecated,弃用
        // we keep it as is in order to be compatible//将从buffer_ctx起始地址开始的一块数据赋值为msg->meta
        UnpackMeta(buffer_ctx->buffer, buffer_ctx->meta_len, &msg->meta);
        int meta_len = GetPackMetaLen(msg->meta);

        int total_len = 0;
        total_len += meta_len;

        auto trans = CHECK_NOTNULL(endpoint->GetTransport());

        PrintRecvLog(msg, buffer_ctx, meta_len);

        if (!IsValidPushpull(*msg)) {
            return total_len;
        }

        // valid data message
        if (msg->meta.push && msg->meta.request) {
            // push request
            total_len += trans->RecvPushRequest(msg, buffer_ctx, meta_len);
        } else if (!msg->meta.push && msg->meta.request) {
            // pull request
            total_len += trans->RecvPullRequest(msg, buffer_ctx, meta_len);
        } else if (msg->meta.push && !msg->meta.request) {
            // push response
            total_len += trans->RecvPushResponse(msg, buffer_ctx, meta_len);
        } else if (!msg->meta.push && !msg->meta.request) {
            // pull response
            total_len += trans->RecvPullResponse(msg, buffer_ctx, meta_len);
        } else {
            CHECK(0) << "unknown msg type";
        }

        return total_len;
    }

private:
    void PrintSendLog(Message &msg, MessageBuffer *msg_buf, RemoteTuple remote_tuple)
    {
        if (!enable_log_)
            return;
        std::lock_guard<std::mutex> lock(log_mu_);

        if (!IsValidPushpull(msg)) {
            LOG(INFO) << "Send Control Message" << std::flush;
        } else if (msg.meta.push && msg.meta.request) {
            // worker, push request
            LOG(INFO) << "Send Push Request: key=" << msg.meta.key << "\t timestamp=" << msg.meta.timestamp
                      << "\t recver=" << msg.meta.recver << "\t tensor_len=" << msg_buf->mrs[0].second
                      << "\t remote_idx=" << std::get<2>(remote_tuple) << "\t remote_addr=" << std::get<0>(remote_tuple)
                      << std::flush;
        } else if (msg.meta.push && !msg.meta.request) {
            // server, push response
            LOG(INFO) << "Send Push Response: key=" << msg.meta.key << "\t timestamp=" << msg.meta.timestamp
                      << "\t recver=" << msg.meta.recver << "\t remote_idx=" << std::get<2>(remote_tuple)
                      << "\t remote_addr=" << std::get<0>(remote_tuple) << std::flush;
        } else if (!msg.meta.push && msg.meta.request) {
            // worker, pull request
            LOG(INFO) << "Send Pull Request: key=" << msg.meta.key << "\t timestamp=" << msg.meta.timestamp
                      << "\t recver=" << msg.meta.recver << "\t remote_idx=" << std::get<2>(remote_tuple)
                      << "\t remote_addr=" << std::get<0>(remote_tuple) << std::flush;
        } else if (!msg.meta.push && !msg.meta.request) {
            // server, pull response
            LOG(INFO) << "Send Pull Response: key=" << msg.meta.key << "\t timestamp=" << msg.meta.timestamp
                      << "\t recver=" << msg.meta.recver << "\t tensor_len=" << msg.meta.val_len << "\t idx="
                      << "none"
                      << "\t remote_addr=" << msg.meta.addr << std::flush;
        }
    }

    void PrintRecvLog(Message *msg, BufferContext *buffer_ctx, int meta_len)
    {
        if (!enable_log_)
            return;
        std::lock_guard<std::mutex> lock(log_mu_);

        if (!IsValidPushpull(*msg)) {
            LOG(INFO) << "Recv Control Message" << std::flush;
        } else if (msg->meta.push && msg->meta.request) {
            // push request
            LOG(INFO) << "Recv Push Request: key=" << msg->meta.key << "\t timestamp=" << msg->meta.timestamp
                      << "\t sender=" << msg->meta.sender << "\t tensor_len=" << buffer_ctx->data_len[1] << std::flush;
        } else if (!msg->meta.push && msg->meta.request) {
            // pull request
            LOG(INFO) << "Recv Pull Request: key=" << msg->meta.key << "\t timestamp=" << msg->meta.timestamp
                      << "\t sender=" << msg->meta.sender << std::flush;
        } else if (msg->meta.push && !msg->meta.request) {
            // push response
            LOG(INFO) << "Recv Push Response: key=" << msg->meta.key << "\t timestamp=" << msg->meta.timestamp
                      << "\t sender=" << msg->meta.sender << std::flush;
        } else if (!msg->meta.push && !msg->meta.request) {
            // pull response
            LOG(INFO) << "Recv Pull Response: key=" << msg->meta.key << "\t timestamp=" << msg->meta.timestamp
                      << "\t sender=" << msg->meta.sender << "\t tensor_len=" << msg->meta.val_len;
        }
    }

    bool HasRemoteInfo(Message &msg, uint64_t key, bool is_push, int recver)
    {
        std::lock_guard<std::mutex> lk(addr_mu_);
        if (is_push && (push_addr_.find(key) != push_addr_.end()) &&
            (push_addr_[key].find(recver) != push_addr_[key].end())) {
            return true;
        }
        if (!is_push && (pull_addr_.find(key) != pull_addr_.end()) &&
            (pull_addr_[key].find(recver) != pull_addr_[key].end())) {
            return true;
        }

        return false;
    }

    void StoreMsgBuf(MessageBuffer *msg_buf, Message &msg)
    {
        std::lock_guard<std::mutex> lk(addr_mu_);
        CHECK_EQ(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
        msgbuf_cache_[msg_buf] = msg;
    }

    Message *GetFirstMsg(MessageBuffer *msg_buf)
    {
        std::lock_guard<std::mutex> lk(addr_mu_);
        CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
        return &msgbuf_cache_[msg_buf];
    }

    void ReleaseFirstMsg(MessageBuffer *msg_buf)
    {
        std::lock_guard<std::mutex> lk(addr_mu_);
        CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
        msgbuf_cache_.erase(msg_buf);
    }

    void StoreRemoteAndLocalInfo(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx)
    {
        std::lock_guard<std::mutex> lk(addr_mu_);

        CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());

        auto &msg = msgbuf_cache_[msg_buf];

        auto key = msg.meta.key;
        auto is_push = msg.meta.push;
        auto recver = msg.meta.recver;

        auto t = std::make_tuple(remote_addr, rkey, idx, msg_buf);
        if (is_push) {
            push_addr_[key][recver] = t;
        } else {
            pull_addr_[key][recver] = t;
        }
    }

    RemoteTuple GetRemoteAndLocalInfo(uint64_t key, bool is_push, int recver)
    {
        std::lock_guard<std::mutex> lk(addr_mu_);
        return (is_push ? push_addr_[key][recver] : pull_addr_[key][recver]);
    }

    MessageBuffer *PrepareNewMsgBuf(Message &msg)
    {
        MessageBuffer *msg_buf = new MessageBuffer();
        auto meta_len = GetPackMetaLen(msg.meta);  //获取元数据的长度
        msg_buf->inline_len = meta_len;
        msg_buf->inline_buf = mem_allocator_->Alloc(meta_len);  //注册meta_len大小的内存
        msg_buf->data = msg.data;                               // data包含keys,vals,lens
        PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);  //将msg.meta拷贝到msg_buf->inline_buf开始的一段内存
        return msg_buf;
    }

    void RegisterMemory(Message &msg)
    {
        size_t sa_cnt = 0;
        for (auto &sa : msg.data) {  // msg.data是一个vector数组，即不一定是连续的数据块，
            if (sa.size() == 0)
                continue;
            std::lock_guard<std::mutex> lock(map_mu_);  // sa.data()即返回sa保存的指针，data应该分为keys,vals,lens
            if ((mem_mr_.find(sa.data()) == mem_mr_.end()) && (sa_cnt == 1)) {  // only vals register memory
                struct ibv_mr *temp_mr;
                CHECK(temp_mr = ibv_reg_mr(mem_allocator_->GetPD(),
                          sa.data(),
                          sa.size(),
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
                    << "Failed to register the memory region: " << strerror(errno) << ", sa.size()=" << sa.size();
                mem_mr_[sa.data()] = temp_mr;
            }
            ++sa_cnt;
        }
        // register for tensor address of pull request,控制信息不为空，作为worker注册用于PULL回来的数据存放的内存
        if (IsValidPushpull(msg) && !msg.meta.push && msg.meta.request) {
            CHECK_GT(msg.meta.val_len, 0) << msg.meta.val_len;
            auto addr = reinterpret_cast<char *>(msg.meta.addr);
            std::lock_guard<std::mutex> lock(map_mu_);
            if (mem_mr_.find(addr) == mem_mr_.end()) {
                struct ibv_mr *temp_mr;
                CHECK(temp_mr = ibv_reg_mr(mem_allocator_->GetPD(),
                          addr,
                          msg.meta.val_len,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
                    << "Failed to register the memory region: " << strerror(errno);
                mem_mr_[addr] = temp_mr;
            }
        }
    }

    void PrepareData(Message &msg, MessageBuffer *msg_buf)
    {
        if (!(msg.meta.push && msg.meta.request))
            return;  // only push request
        auto &sa = msg_buf->data[1];
        if (sa.size() == 0)
            return;
        std::lock_guard<std::mutex> lock(map_mu_);
        auto it = mem_mr_.find(sa.data());
        CHECK_NE(it, mem_mr_.end());
        MRPtr ptr(it->second, [](struct ibv_mr *mr) {});
        CHECK(ptr.get()) << strerror(errno);
        msg_buf->mrs.push_back(std::make_pair(std::move(ptr), sa.size()));
    }

    void AddMeta(Message &msg)
    {
        if (msg.meta.request) {
            msg.meta.key = DecodeKey(msg.data[0]);
        }
        if (!msg.meta.push && msg.meta.request) {
            // pull request
            std::lock_guard<std::mutex> lock(map_mu_);
            auto val_addr = reinterpret_cast<char *>(msg.meta.addr);
            msg.meta.option = mem_mr_[val_addr]->rkey;
        }
    }
    void MulticastInitContext(struct ibv_context *context)
    {
        m_context_ = context;
        CHECK(m_context_) << "ibv_context * empty";
        m_pd_ = ibv_alloc_pd(m_context_);
        CHECK(m_pd_) << "Failed to allocate protection domain";

        m_mem_allocator_.reset(new MemoryAllocator(m_pd_));
        m_comp_event_channel_ = ibv_create_comp_channel(m_context_);

        m_cq_ = ibv_create_cq(m_context_, kMaxConcurrentWorkRequest * 2, NULL, comp_event_channel_, 0);

        CHECK(m_cq_) << "Failed to create completion queue";
        CHECK(!ibv_req_notify_cq(m_cq_, 0))
            << "Failed to request CQ notification";  //当CQE在CQ中产生时，一个完成事件将会产生，用户使用ibv_get_cq_event操作来接收通知
    }
    void InitContext(struct ibv_context *context)
    {
        context_ = context;
        CHECK(context_) << "ibv_context* empty";

        pd_ = ibv_alloc_pd(context_);
        CHECK(pd_) << "Failed to allocate protection domain";

        mem_allocator_.reset(new MemoryAllocator(pd_));

        comp_event_channel_ = ibv_create_comp_channel(
            context_);  //创建一个完成通道，完成通道是一种机制，当新的CQE被放置在CQ上时，用户可以接收通知

        // TODO(clan): Replace the rough estimate here
        cq_ = ibv_create_cq(context_, kMaxConcurrentWorkRequest * 2, NULL, comp_event_channel_, 0);

        CHECK(cq_) << "Failed to create completion queue";
        CHECK(!ibv_req_notify_cq(cq_, 0))
            << "Failed to request CQ notification";  //当CQE在CQ中产生时，一个完成事件将会产生，用户使用ibv_get_cq_event操作来接收通知
    }

    void ReleaseWorkRequestContext(WRContext *context, Endpoint *endpoint)
    {
        switch (context->type) {
            case kRendezvousStartContext:
                endpoint->free_start_ctx.Push(context);
                break;
            case kRendezvousReplyContext:
                endpoint->free_reply_ctx.Push(context);
                break;
            case kReceiveContext:
                endpoint->PostRecv(context);  //将wr塞进接收队列
                break;
            default:
                CHECK(0);
        }
    }

    void m_PollCQ()
    {
        // Pre-allocated work completions array used for polling
        struct ibv_wc wc[kMaxConcurrentWorkRequest];
        while (!should_stop_.load()) {
            break;
            int ne = ibv_poll_cq(m_cq_, kMaxConcurrentWorkRequest, wc);
            CHECK_GE(ne, 0);
            for (int i = 0; i < ne; ++i) {
                LOG(INFO) << " this is multicast pollcq ";
                CHECK(wc[i].status == IBV_WC_SUCCESS) << "Failed status \n"
                                                      << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
                                                      << static_cast<uint64_t>(wc[i].wr_id) << " " << wc[i].vendor_err;

                WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
                Endpoint *endpoint =
                    reinterpret_cast<Endpoint *>(context->private_data);  // private_data指向一个endpoint的指针？
                // endpoint即得到了这个连接，不管是作为server还是client。

                // IBV_WC_RDMA_WRITE use msg_buf as the wr_id
                // so there won't be context and endpoint for this op
                switch (wc[i].opcode) {
                    case IBV_WC_SEND: {  // Send operation for a wr that was posted to the Send Queue
                        ReleaseWorkRequestContext(context, endpoint);
                    } break;
                    case IBV_WC_RDMA_WRITE: {  // RDMA write operation for a WR that was posted to the Send Queue
                        // do nothing，完成了一个写操作
                    } break;
                    case IBV_WC_RECV_RDMA_WITH_IMM: {  // RDMA with immediate for a WR that was posted to a Receive
                                                       // Queue
                        uint32_t addr_idx = wc[i].imm_data;
                        BufferContext *buf_ctx = addr_pool_.GetAddress(addr_idx);
                        recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
                        ReleaseWorkRequestContext(context, endpoint);
                    } break;
                    case IBV_WC_RECV: {  // Send data operation for a WR that was posted to a Receive
                                         // Queue,即收到数据
                        CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
                        uint32_t imm = wc[i].imm_data;
                        struct ibv_mr *mr = context->buffer;

                        if (imm ==
                            kRendezvousStart) {  //表示是对端第一次发过来的消息，对端此时采用的是send，需要将本端的地址和rkey发送至对方，比如client首次push数据
                            RendezvousStart *
                                req =  // reinterpret允许将任何指针转换为其他指针类型。它允许将任何整数类型转换为任何指针以及反向转换。
                                reinterpret_cast<RendezvousStart *>(mr->addr);
                            auto trans = CHECK_NOTNULL(endpoint->GetTransport());
                            trans->SendRendezvousReply(req, addr_pool_);

                        } else if (imm == kRendezvousReply) {  //表示收到对端发来的地址信息
                            RendezvousReply *resp = reinterpret_cast<RendezvousReply *>(mr->addr);
                            uint64_t remote_addr = resp->addr;
                            uint64_t origin_addr = resp->origin_addr;
                            uint32_t rkey = resp->rkey;
                            uint32_t idx = resp->idx;

                            MessageBuffer *msg_buf = reinterpret_cast<MessageBuffer *>(origin_addr);

                            // Before RDMA write, store the remote info so that
                            // subsequent write does not need repeated
                            // rendezvous//将对端需要的key的地址保存下来，下次发送该key的数据时可以直接写
                            StoreRemoteAndLocalInfo(msg_buf, remote_addr, rkey, idx);

                            Message *msg = GetFirstMsg(msg_buf);  //返回用于接收数据的内存的首地址

                            auto addr_tuple = GetRemoteAndLocalInfo(msg->meta.key, msg->meta.push, msg->meta.recver);

                            PrintSendLog(*msg, msg_buf, addr_tuple);

                            auto trans = CHECK_NOTNULL(endpoint->GetTransport());
                            if (!IsValidPushpull(*msg)) {
                                // control message
                                trans->RDMAWriteWithImm(msg_buf,
                                    remote_addr,
                                    rkey,
                                    idx);  //收到对端写过来的地址信息，需要将自己本端的地址信息和rkey也告知对端
                            } else if (msg->meta.push && msg->meta.request) {
                                // worker, push request
                                trans->SendPushRequest(*msg, msg_buf, addr_tuple);
                            } else if (msg->meta.push && !msg->meta.request) {
                                // server, push response
                                trans->SendPushResponse(*msg, msg_buf, addr_tuple);
                            } else if (!msg->meta.push && msg->meta.request) {
                                // worker, pull request
                                trans->SendPullRequest(*msg, msg_buf, addr_tuple);
                            } else if (!msg->meta.push && !msg->meta.request) {
                                // server, pull response
                                map_mu_.lock();
                                auto temp_mr = mem_mr_.find(msg_buf->data[1].data());
                                CHECK_NE(temp_mr, mem_mr_.end());
                                map_mu_.unlock();
                                trans->SendPullResponse(*msg, msg_buf, addr_tuple, temp_mr->second->lkey);
                            }

                            // release the msg_buf from msgbuf_cache_
                            ReleaseFirstMsg(msg_buf);

                        } else {
                            CHECK(0);
                        }
                        ReleaseWorkRequestContext(context, endpoint);
                    } break;
                    default:
                        CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
                }
            }
        }
    }
    // PollCQ
    void PollCQ()
    {
        // Pre-allocated work completions array used for polling
        struct ibv_wc wc[kMaxConcurrentWorkRequest];
        while (!should_stop_.load()) {
            int ne = ibv_poll_cq(cq_, kMaxConcurrentWorkRequest, wc);
            CHECK_GE(ne, 0);
            for (int i = 0; i < ne; ++i) {
                CHECK(wc[i].status == IBV_WC_SUCCESS) << "Failed status \n"
                                                      << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
                                                      << static_cast<uint64_t>(wc[i].wr_id) << " " << wc[i].vendor_err;

                WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
                Endpoint *endpoint =
                    reinterpret_cast<Endpoint *>(context->private_data);  // private_data指向一个endpoint的指针？
                // endpoint即得到了这个连接，不管是作为server还是client。

                // IBV_WC_RDMA_WRITE use msg_buf as the wr_id
                // so there won't be context and endpoint for this op

                switch (wc[i].opcode) {
                    case IBV_WC_SEND: {  // Send operation for a wr that was posted to the Send Queue
                        ReleaseWorkRequestContext(context, endpoint);
                    } break;
                    case IBV_WC_RDMA_WRITE: {  // RDMA write operation for a WR that was posted to the Send Queue
                        // do nothing，完成了一个写操作
                    } break;
                    case IBV_WC_RECV_RDMA_WITH_IMM: {  // RDMA with immediate for a WR that was posted to a Receive
                                                       // Queue
                        uint32_t addr_idx = wc[i].imm_data;
                        BufferContext *buf_ctx = addr_pool_.GetAddress(addr_idx);
                        recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
                        ReleaseWorkRequestContext(context, endpoint);
                    } break;
                    case IBV_WC_RECV: {  // Send data operation for a WR that was posted to a Receive
                                         // Queue,即收到数据
                        CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
                        uint32_t imm = wc[i].imm_data;
                        struct ibv_mr *mr = context->buffer;

                        if (imm ==
                            kRendezvousStart) {  //表示是对端第一次发过来的消息，对端此时采用的是send，需要将本端的地址和rkey发送至对方，比如client首次push数据
                            RendezvousStart *
                                req =  // reinterpret允许将任何指针转换为其他指针类型。它允许将任何整数类型转换为任何指针以及反向转换。
                                reinterpret_cast<RendezvousStart *>(mr->addr);
                            auto trans = CHECK_NOTNULL(endpoint->GetTransport());
                            trans->SendRendezvousReply(req, addr_pool_);

                        } else if (imm == kRendezvousReply) {  //表示收到对端发来的地址信息
                            RendezvousReply *resp = reinterpret_cast<RendezvousReply *>(mr->addr);
                            uint64_t remote_addr = resp->addr;
                            uint64_t origin_addr = resp->origin_addr;
                            uint32_t rkey = resp->rkey;
                            uint32_t idx = resp->idx;

                            MessageBuffer *msg_buf = reinterpret_cast<MessageBuffer *>(origin_addr);

                            // Before RDMA write, store the remote info so that
                            // subsequent write does not need repeated
                            // rendezvous//将对端需要的key的地址保存下来，下次发送该key的数据时可以直接写
                            StoreRemoteAndLocalInfo(msg_buf, remote_addr, rkey, idx);

                            Message *msg = GetFirstMsg(msg_buf);  //返回用于接收数据的内存的首地址

                            auto addr_tuple = GetRemoteAndLocalInfo(msg->meta.key, msg->meta.push, msg->meta.recver);

                            PrintSendLog(*msg, msg_buf, addr_tuple);

                            auto trans = CHECK_NOTNULL(endpoint->GetTransport());
                            if (!IsValidPushpull(*msg)) {
                                // control message
                                trans->RDMAWriteWithImm(msg_buf,
                                    remote_addr,
                                    rkey,
                                    idx);  //收到对端写过来的地址信息，需要将自己本端的地址信息和rkey也告知对端
                            } else if (msg->meta.push && msg->meta.request) {
                                // worker, push request
                                trans->SendPushRequest(*msg, msg_buf, addr_tuple);
                            } else if (msg->meta.push && !msg->meta.request) {
                                // server, push response
                                trans->SendPushResponse(*msg, msg_buf, addr_tuple);
                            } else if (!msg->meta.push && msg->meta.request) {
                                // worker, pull request
                                trans->SendPullRequest(*msg, msg_buf, addr_tuple);
                            } else if (!msg->meta.push && !msg->meta.request) {
                                // server, pull response
                                map_mu_.lock();
                                auto temp_mr = mem_mr_.find(msg_buf->data[1].data());
                                CHECK_NE(temp_mr, mem_mr_.end());
                                map_mu_.unlock();
                                trans->SendPullResponse(*msg, msg_buf, addr_tuple, temp_mr->second->lkey);
                            }

                            // release the msg_buf from msgbuf_cache_
                            ReleaseFirstMsg(msg_buf);

                        } else {
                            CHECK(0);
                        }
                        ReleaseWorkRequestContext(context, endpoint);
                    } break;
                    default:
                        CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
                }
            }
        }
    }

    void PollEvents()
    {
        int flags = fcntl(event_channel_->fd, F_GETFL);  // flags获得文件状态标记
        int rc = fcntl(event_channel_->fd, F_SETFL, flags | O_NONBLOCK);
        CHECK_GE(rc, 0);
        int error_flags = POLLERR | POLLHUP | POLLNVAL;

        while (!should_stop_.load()) {
            struct pollfd pfd = {.fd = event_channel_->fd, .events = POLLIN, .revents = 0};
            int ret = poll(&pfd, 1, 10);

            CHECK_GE(ret, 0) << strerror(errno);
            CHECK_EQ(pfd.revents & error_flags, 0);

            if (!(pfd.revents & POLLIN)) {
                continue;
            }

            struct rdma_cm_event *event;
            CHECK_EQ(rdma_get_cm_event(event_channel_, &event), 0);
            // TODO(clan): Reorder the list according to the event frequency，此处相当于将client和server写到了一起
            switch (event->event) {
                case RDMA_CM_EVENT_CONNECT_REQUEST:  // server
                    OnConnectRequest(event);
                    break;
                case RDMA_CM_EVENT_ADDR_RESOLVED:  // client
                    OnAddrResolved(event);
                    break;
                case RDMA_CM_EVENT_MULTICAST_JOIN:  //对应于ud创建ah
                    OnJoinMulticastHandler(event);
                    break;
                case RDMA_CM_EVENT_ROUTE_RESOLVED:  // client，代表rdma_resolve_route路由解析成功完成
                    OnRouteResolved(event);
                    break;
                case RDMA_CM_EVENT_ESTABLISHED:  // client，server
                    OnConnected(event);
                    break;
                case RDMA_CM_EVENT_DISCONNECTED:  // client，server
                    OnDisconnected(event);
                    break;
                case RDMA_CM_EVENT_REJECTED:
                    OnRejected(event);
                    break;
                default:
                    CHECK(0) << "OnEvent: unknown event " << event->event << " (" << rdma_event_str(event->event)
                             << ")";
            }
            rdma_ack_cm_event(event);
        }
    }

    void OnRejected(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

        endpoints_mu_.lock();
        auto it = endpoints_.find(endpoint->node_id);
        CHECK(it != endpoints_.end()) << "Connection not ready.";
        endpoints_mu_.unlock();

        CHECK_EQ(endpoint->status, Endpoint::CONNECTING);
        CHECK_EQ(endpoint->cm_id, id);

        PS_VLOG(1) << "Connection rejected, retrying...";
        {
            std::lock_guard<std::mutex> lk(endpoint->connect_mu);
            endpoint->status = Endpoint::REJECTED;
        }
        endpoint->cv.notify_all();
    }

    void OnConnectRequest(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        CHECK_NOTNULL(id);

        CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
            << "RequestContext size mismatch. Actual: " << (size_t)event->param.conn.private_data_len
            << ", Expected: " << sizeof(RequestContext);
        CHECK_NOTNULL(event->param.conn.private_data);

        const RequestContext *remote_ctx = reinterpret_cast<const RequestContext *>(
            event->param.conn
                .private_data);  // private_data字段为用户调用rdma_connect或rdma_accept时指定的结构体，此处指定了一个RequestContext

        const auto r =
            incoming_.emplace(std::make_unique<Endpoint>());  //因为自己是server，所以会有client向自己发起一个连接
        Endpoint *endpoint = r.first->get();                  //一个Endpoint表示一个rdma连接
        endpoint->SetNodeID(remote_ctx->node);                //表示对端的node信息
        endpoint->cm_id = id;
        id->context = endpoint;

        if (context_ == nullptr) {
            InitContext(id->verbs);  //初始化资源，ibv_create_comp_channel
        }
        // wr.wr_id = reinterpret_cast<uint64_t>(ctx);
        //表示endpoint对应的ctx表示wr.wr_id，在pollcq的时候通过将wr_id转化为endpoint即可得到这个连接
        endpoint->Init(cq_, pd_);  //在本端初始化这个endpoint连接的信息

        bool is_local_node =
            disable_ipc_ ? false : (std::string(remote_ctx->hostname) == my_node_.hostname ? true : false);
        {
            std::lock_guard<std::mutex> lk(local_mu_);
            is_local_[remote_ctx->node] = is_local_node;
        }
        LOG(INFO) << "OnConnect to Node " << remote_ctx->node << " with Transport=" << (is_local_node ? "IPC" : "RDMA");

        std::shared_ptr<Transport> t = is_local_node ? std::make_shared<IPCTransport>(endpoint, mem_allocator_.get())
                                                     : std::make_shared<RDMATransport>(endpoint, mem_allocator_.get());
        endpoint->SetTransport(t);

        RequestContext ctx;
        ctx.node = static_cast<uint32_t>(my_node_.id);
        ctx.port = static_cast<uint16_t>(my_node_.port);
        snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

        struct rdma_conn_param cm_params;
        memset(&cm_params, 0, sizeof(cm_params));
        cm_params.retry_count = 7;
        cm_params.rnr_retry_count = 7;
        cm_params.private_data = &ctx;
        cm_params.private_data_len = sizeof(RequestContext);

        CHECK_EQ(rdma_accept(id, &cm_params), 0)  //在监听端调用rdma_accept来接收连接
            << "Accept RDMA connection failed: " << strerror(errno);
    }

    void OnJoinMulticastHandler(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        struct rdma_ud_param *param = &event->param.ud;
        Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
        char buf[40];
        inet_ntop(AF_INET6, param->ah_attr.grh.dgid.raw, buf, 40);
        printf("mckey:join dgid:%s\n", buf);
        endpoint->remote_qpn = param->qp_num;
        endpoint->remote_qkey = param->qkey;

        RequestContext ctx;
        ctx.node = static_cast<uint32_t>(my_node_.id);
        ctx.port = static_cast<uint16_t>(my_node_.port);
        snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());
        param->private_data = &ctx;
        param->private_data_len = sizeof(RequestContext);

        endpoint->ah = ibv_create_ah(pd_, &param->ah_attr);
        if (!endpoint->ah) {
            printf("mckey: failure creating address handle\n");
            return;
        }

        if (m_cq_polling_thread_ == nullptr) {  //如果此时负责poll cq的线程还未开启，则开启
            m_cq_polling_thread_.reset(new std::thread(&RDMAVan::m_PollCQ, this));
        }

        CHECK_EQ(endpoint->cm_id, id);
        {
            std::lock_guard<std::mutex> lk(endpoint->connect_mu);
            endpoint->status = Endpoint::CONNECTED;
        }
        LOG(INFO) << "need to notify_all every endpoint";
        endpoint->cv.notify_all();
        // if (endpoint->node_id != my_node_.id) {
        //    PS_VLOG(1) << "OnConnected to Node " << endpoint->node_id;
        //}
    }

    void LeaveMulticast(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
        struct addrinfo *remote_addr;
        std::string addr = endpoint->multicast_address;

        CHECK_EQ(getaddrinfo(addr.c_str(), nullptr, nullptr, &remote_addr), 0);  //离开该组播组的连接
        if (rdma_leave_multicast(id, remote_addr->ai_addr)) {
            printf("leave multicaset failed \n");
            return;
        }
    }
    // Resolve a route after address is resolved
    void OnAddrResolved(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        auto it = std::find(listen_id_list.begin(), listen_id_list.end(), event->id);
        // if there is an endpoint with pending connection
        if (it == listen_id_list.end()) {
            // LOG(INFO) << "tradtional rdma connect OnAddrResolved \n";
            CHECK_EQ(rdma_resolve_route(id, kTimeoutms), 0)  //将RDMA路由解析到目的地址，以便建立连接。
                << "Resolve RDMA route failed";
        } else {
            // LOG(INFO) << "multicast OnAddrResolved \n"; //对应组播连接的情况
            Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);  //对应组播的连接

            if (m_context_ == nullptr) {
                MulticastInitContext(id->verbs);
            }

            //计算最大mtu
            struct ibv_port_attr port_attr;
            int ret = ibv_query_port(id->verbs, id->port_num, &port_attr);
            int len = 1 << (port_attr.active_mtu + 7);  // len表示最大mtu长度

            endpoint->MulticastInit(m_cq_, m_pd_, len);  //自己作为client，初始化cq和pd

            RequestContext ctx;
            ctx.node = static_cast<uint32_t>(my_node_.id);
            ctx.port = static_cast<uint16_t>(my_node_.port);
            snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

            struct addrinfo *remote_addr;
            std::string
                addr;  //找到第一个未加入的组播地址，然后加入。如果是server，则未加入的组播地址只有一个，即为绑定自己的组播地址
            //如果是worker，则未加入的组播地址的个数为server的个数
            for (auto &c : m_record) {
                if (c.used == false) {
                    addr = c.addr;
                    c.used = true;
                    break;
                }
            }
            endpoint->multicast_address = addr;
            CHECK_EQ(getaddrinfo(addr.c_str(), nullptr, nullptr, &remote_addr), 0);
            if (rdma_join_multicast(id, remote_addr->ai_addr, endpoint)) {
                printf("join multicast failed \n");
                return;
            }
        }
    }

    // Make a connection after route is resolved
    void OnRouteResolved(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);  //将id->context所指的地方赋值到endpoint指针

        if (context_ == nullptr) {
            InitContext(id->verbs);
        }

        endpoint->Init(cq_, pd_);  //自己作为client，初始化cq和pd

        RequestContext ctx;
        ctx.node = static_cast<uint32_t>(my_node_.id);
        ctx.port = static_cast<uint16_t>(my_node_.port);
        snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

        struct rdma_conn_param cm_params;
        memset(&cm_params, 0, sizeof(cm_params));
        cm_params.retry_count = 7;
        cm_params.rnr_retry_count = 7;
        cm_params.private_data = &ctx;
        cm_params.private_data_len = sizeof(RequestContext);

        CHECK_EQ(rdma_connect(id, &cm_params), 0)  //发起一个活动连接请求，
            << "RDMA connect failed" << strerror(errno);
    }

    void OnConnected(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        CHECK(id) << "rdma_cm_id not found.";
        Endpoint *endpoint =
            reinterpret_cast<Endpoint *>(id->context);  //不是只有server端才将endpoint赋值到了id->context么？
        CHECK(endpoint) << "Endpoint not found.";

        if (cq_polling_thread_ == nullptr) {  //如果此时负责poll cq的线程还未开启，则开启
            cq_polling_thread_.reset(new std::thread(&RDMAVan::PollCQ, this));
        }

        CHECK_EQ(endpoint->cm_id, id);
        {
            std::lock_guard<std::mutex> lk(endpoint->connect_mu);
            endpoint->status = Endpoint::CONNECTED;
        }
        endpoint->cv.notify_all();
        if (endpoint->node_id != my_node_.id) {
            PS_VLOG(1) << "OnConnected to Node " << endpoint->node_id;
        }
    }

    void OnDisconnected(struct rdma_cm_event *event)
    {
        struct rdma_cm_id *id = event->id;
        Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
        {
            std::lock_guard<std::mutex> lk(endpoint->connect_mu);
            endpoint->status = Endpoint::IDLE;
        }
        endpoint->cv.notify_all();
        LOG(INFO) << "OnDisconnected from Node " << endpoint->node_id;
    }

    AddressPool<BufferContext> addr_pool_;
    std::unique_ptr<MemoryAllocator> mem_allocator_;
    std::unique_ptr<MemoryAllocator> m_mem_allocator_;

    std::unique_ptr<RDMATransport> rdma_trans_;
    std::unique_ptr<IPCTransport> ipc_trans_;

    struct rdma_cm_id *listener_ = nullptr;
    std::vector<rdma_cm_id *> listen_id_list;
    std::atomic<bool> should_stop_;

    std::mutex endpoints_mu_;
    std::mutex m_endpoints_mu_;
    std::unordered_map<int, std::unique_ptr<Endpoint>> m_endpoints_;  //组播对应的端点
    std::vector<MulticastInfo> m_record;
    std::unordered_map<int, std::unique_ptr<Endpoint>> endpoints_;
    std::unordered_set<std::unique_ptr<Endpoint>> incoming_;

    struct rdma_event_channel *event_channel_ = nullptr;
    struct ibv_context *context_ = nullptr;
    struct ibv_context *m_context_ = nullptr;

    // ibverbs protection domain
    struct ibv_pd *pd_ = nullptr;
    // ibverbs protection domain
    struct ibv_pd *m_pd_ = nullptr;
    // Completion event channel, to wait for work completions
    struct ibv_comp_channel *comp_event_channel_ = nullptr;
    // multicast Completion event channel , to wait for work completions
    struct ibv_comp_channel *m_comp_event_channel_ = nullptr;
    // Completion queue, to poll on work completions
    struct ibv_cq *cq_ = nullptr;
    // multicast Completion queue, to poll on work completions
    struct ibv_cq *m_cq_ = nullptr;
    // cq thread
    std::unique_ptr<std::thread> cq_polling_thread_;
    // multicast cq thread
    std::unique_ptr<std::thread> m_cq_polling_thread_;
    // event thread
    std::unique_ptr<std::thread> cm_event_polling_thread_;
    // Recv buffer queue
    ThreadsafeQueue<std::tuple<Endpoint *, BufferContext *>> recv_buffers_;

    // local IPC related
    bool disable_ipc_ = false;
    std::mutex local_mu_;
    std::unordered_map<int, bool> is_local_;

    std::mutex addr_mu_;
    // <key, recver>, (<remote_addr, rkey, idx, local_addr>)
    std::unordered_map<uint64_t, RemoteAndLocalAddress> push_addr_;
    std::unordered_map<uint64_t, RemoteAndLocalAddress> pull_addr_;
    std::unordered_map<MessageBuffer *, Message> msgbuf_cache_;  // msg_buf, msg

    std::mutex map_mu_;
    std::unordered_map<char *, struct ibv_mr *> mem_mr_;  // (memory address, ibv_mr)

    // logging
    bool enable_log_;
    std::mutex log_mu_;

    int kMaxConcurrentWorkRequest = 4224;  // 128 + 2048 * 2

};  // class RDMAVan

};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_
