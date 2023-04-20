/***********************************************************************
 *
 * store/common/backend/messageserver.h:
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef COMMON_MESSAGE_SERVER_H
#define COMMON_MESSAGE_SERVER_H

#include "lib/transport.h"
#include "store/server.h"

class MessageServer : public TransportReceiver, public ::Server
{
public:
    MessageServer();
    virtual ~MessageServer();

    virtual void ReceiveMessage(const TransportAddress &remote,
                                const std::string &type, const std::string &data,
                                void *meta_data) override;

    virtual inline Stats &GetStats() override { return stats; };

protected:
    typedef void (MessageServer::*MessageHandler)(const TransportAddress &,
                                                  google::protobuf::Message *);

    void RegisterHandler(google::protobuf::Message *message,
                         MessageServer::MessageHandler handler);

    Stats stats;

private:
    std::unordered_map<std::string, std::pair<google::protobuf::Message *,
                                              MessageServer::MessageHandler>>
        msgHandlers;
};

#endif /* COMMON_MESSAGE_SERVER_H */
