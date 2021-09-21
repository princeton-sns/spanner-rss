#ifndef PING_SERVER_H
#define PING_SERVER_H

#include "lib/transport.h"
#include "store/common/common-proto.pb.h"

class PingServer {
 public:
  PingServer(Transport *transport);
  virtual ~PingServer();

  void HandlePingMessage(TransportReceiver *receiver,
      const TransportAddress &remote, const PingMessage &ping);

 private:
  Transport *transport;

};

#endif /* PING_SERVER_H */
