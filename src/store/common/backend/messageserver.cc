#include "store/common/backend/messageserver.h"

MessageServer::MessageServer() {
}

MessageServer::~MessageServer() {
}

void MessageServer::ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data, void *meta_data) {
  auto msgHandlerItr = msgHandlers.find(type);
  if (msgHandlerItr == msgHandlers.end()) {
    Panic("Received unexpected message type: %s", type.c_str());
  }

  msgHandlerItr->second.first->ParseFromString(data);
  (this->*msgHandlerItr->second.second)(remote, msgHandlerItr->second.first);
  //stats.Increment(type);
}

void MessageServer::RegisterHandler(google::protobuf::Message *message,
    MessageHandler handler) {
  msgHandlers.insert(std::make_pair(message->GetTypeName(),
        std::make_pair(message, handler)));
}
