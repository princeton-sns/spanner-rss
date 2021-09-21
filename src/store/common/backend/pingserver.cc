#include "store/common/backend/pingserver.h"

PingServer::PingServer(Transport *transport) : transport(transport) {
}

PingServer::~PingServer() {
}

void PingServer::HandlePingMessage(TransportReceiver *receiver,
    const TransportAddress &remote, const PingMessage &ping) {
  transport->SendMessage(receiver, remote, ping);
}
