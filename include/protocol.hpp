// Simplified ITCH-style binary market data protocol.
//
// This is NOT the real Nasdaq TotalView-ITCH 5.0 spec -- it borrows its core
// idea (a compact, fixed-width binary message stream describing order-book
// events: add/execute/cancel/delete/replace) but simplifies the field list
// and framing so the decoder stays readable. Every message shares a common
// header (type, instrument, timestamp) followed by a type-specific payload.
// All multi-byte integers are native-endian (this project targets x86_64
// only, so no byte-swapping is done -- a real cross-network feed would need
// to pick and document a wire endianness).
#pragma once
#include <cstdint>

namespace mdfeed {

enum class MsgType : uint8_t {
    AddOrder = 'A',
    Execute  = 'E',
    Cancel   = 'X',
    Delete   = 'D',
    Replace  = 'U',
};

enum class Side : uint8_t { Buy = 'B', Sell = 'S' };

#pragma pack(push, 1)

struct MsgHeader {
    MsgType type;
    uint16_t instrument_id;   // 0 .. NUM_INSTRUMENTS-1
    uint64_t timestamp_ns;    // sender-side send timestamp (steady_clock ns)
};

struct AddOrderMsg {
    MsgHeader header;
    uint64_t order_id;
    Side side;
    int64_t price_ticks;      // price in integer cents (1 tick == $0.01)
    uint32_t qty;
};

struct ExecuteMsg {
    MsgHeader header;
    uint64_t order_id;
    uint32_t exec_qty;
};

struct CancelMsg {
    MsgHeader header;
    uint64_t order_id;
    uint32_t cancel_qty;      // partial cancel (reduces resting qty)
};

struct DeleteMsg {
    MsgHeader header;
    uint64_t order_id;
};

struct ReplaceMsg {
    MsgHeader header;
    uint64_t old_order_id;
    uint64_t new_order_id;
    int64_t price_ticks;
    uint32_t qty;
};

// Largest possible message on the wire -- used to size read buffers.
union WireMsg {
    MsgHeader header;
    AddOrderMsg add;
    ExecuteMsg exec;
    CancelMsg cancel;
    DeleteMsg del;
    ReplaceMsg replace;
};

#pragma pack(pop)

constexpr size_t msg_size(MsgType t) {
    switch (t) {
        case MsgType::AddOrder: return sizeof(AddOrderMsg);
        case MsgType::Execute:  return sizeof(ExecuteMsg);
        case MsgType::Cancel:   return sizeof(CancelMsg);
        case MsgType::Delete:   return sizeof(DeleteMsg);
        case MsgType::Replace:  return sizeof(ReplaceMsg);
    }
    return 0;
}

constexpr int NUM_INSTRUMENTS = 512;
constexpr double PRICE_TICK = 0.01; // one tick == one cent

} // namespace mdfeed
