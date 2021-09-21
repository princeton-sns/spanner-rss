#ifndef COMMON_MESSAGE_SERVER_H
#define COMMON_MESSAGE_SERVER_H

#include "store/server.h"
#include "lib/transport.h"

class MessageServer : public TransportReceiver, public ::Server {
 public:
  MessageServer();
  virtual ~MessageServer();

  virtual void ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data,
      void *meta_data) override;

  virtual inline Stats &GetStats() override { return stats; };

 protected:
  typedef void(MessageServer::*MessageHandler)(const TransportAddress &,
      google::protobuf::Message *);

  void RegisterHandler(google::protobuf::Message * message,
      MessageServer::MessageHandler handler);

  Stats stats;

 private:
  std::unordered_map<std::string, std::pair<google::protobuf::Message *,
      MessageServer::MessageHandler>> msgHandlers;
};

#endif /* COMMON_MESSAGE_SERVER_H */
