/***********************************************************************
 *
 * store/common/backend/messageserver.cc:
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
#include "store/common/backend/messageserver.h"

MessageServer::MessageServer()
{
}

MessageServer::~MessageServer()
{
}

void MessageServer::ReceiveMessage(const TransportAddress &remote,
                                   const std::string &type, const std::string &data, void *meta_data)
{
  auto msgHandlerItr = msgHandlers.find(type);
  if (msgHandlerItr == msgHandlers.end())
  {
    Panic("Received unexpected message type: %s", type.c_str());
  }

  msgHandlerItr->second.first->ParseFromString(data);
  (this->*msgHandlerItr->second.second)(remote, msgHandlerItr->second.first);
  // stats.Increment(type);
}

void MessageServer::RegisterHandler(google::protobuf::Message *message,
                                    MessageHandler handler)
{
  msgHandlers.insert(std::make_pair(message->GetTypeName(),
                                    std::make_pair(message, handler)));
}
