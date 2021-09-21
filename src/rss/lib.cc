
#include <rss/lib.h>

#include <iostream>
#include <stdexcept>

namespace rss {

static RSSRegistry RSS_REGISTRY;

void RegisterRSSService(const std::string &name, barrier_func_t bf) {
    RSS_REGISTRY.RegisterRSSService(name, bf);
}
void UnregisterRSSService(const std::string &name) {
    RSS_REGISTRY.UnregisterRSSService(name);
}

void StartTransaction(const std::string &name, Session &s, continuation_func_t continuation) {
    s.StartTransaction(name, continuation);
}

void EndTransaction(const std::string &name, Session &s) {
    s.EndTransaction(name);
}

std::atomic<std::uint64_t> Session::next_id_{0};

Session::Session() : id_{next_id_++}, last_service_{""}, current_state_{NONE} {
    std::cerr << "Session created: " + std::to_string(id_) << std::endl;
    std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
}

Session::Session(Session &&o)
    : id_{o.id_}, last_service_{o.last_service_}, current_state_{o.current_state_} {
    o.id_ = static_cast<uint64_t>(-1);
    o.last_service_ = "";
    o.current_state_ = NONE;
    std::cerr << "Session continued: " + std::to_string(id_) << std::endl;
    std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
}

Session::~Session() {
    std::cerr << "Session destroyed: " + std::to_string(id_) << std::endl;
    std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
}

void Session::StartTransaction(const std::string &name, continuation_func_t continuation) {
    bool invoke_barrier = (!last_service_.empty() && last_service_ != name && current_state_ == EXECUTED);

    switch (current_state_) {
        case NONE:
        case EXECUTED:
            current_state_ = EXECUTING;
            break;
        case EXECUTING:
            std::cerr << "Invalid state transition: Already executing transaction" << std::endl;
            std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
            throw new std::runtime_error("Invalid state transition: Already executing transaction");
        default:
            throw new std::runtime_error("Unexpected state: " + std::to_string(current_state_));
    }

    if (invoke_barrier) {
        auto last = RSS_REGISTRY.FindService(last_service_);
        last_service_ = name;
        last.invoke_barrier(*this, continuation);
    } else {
        last_service_ = name;
        continuation();
    }
}

void Session::EndTransaction(const std::string &name) {
    switch (current_state_) {
        case EXECUTING:
            current_state_ = EXECUTED;
            break;
        case NONE:
        case EXECUTED:
            std::cerr << "Invalid state transition: Not executing transaction" << std::endl;
            std::cerr << "last_service: " << last_service_ << ", current_state: " << static_cast<int>(current_state_) << std::endl;
            throw new std::runtime_error("Invalid state transition: Not executing transaction");
        default:
            throw new std::runtime_error("Unexpected state: " + std::to_string(current_state_));
    }
}

RSSRegistry::RSSService::RSSService(std::string name, barrier_func_t bf)
    : name_{name}, bf_{bf} {}

RSSRegistry::RSSService::~RSSService() {}

RSSRegistry::RSSRegistry() : services_{} {}

RSSRegistry::~RSSRegistry() {}

void RSSRegistry::RegisterRSSService(const std::string &name, barrier_func_t bf) {
    auto search = services_.find(name);
    if (search != services_.end()) {
        throw new std::runtime_error("Duplicate service registration: " + name);
    }

    services_.insert({name, {name, bf}});
}

void RSSRegistry::UnregisterRSSService(const std::string &name) {
    auto search = services_.find(name);
    if (search == services_.end()) {
        throw new std::runtime_error("Service not found: " + name);
    }

    services_.erase(search);
}

RSSRegistry::RSSService &RSSRegistry::FindService(const std::string &name) {
    auto search = services_.find(name);
    if (search == services_.end()) {
        std::cerr << "Service not found: " << name << std::endl;
        throw new std::runtime_error("Service not found: " + name);
    }

    return search->second;
}

}  // namespace rss