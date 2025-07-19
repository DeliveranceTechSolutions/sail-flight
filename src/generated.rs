pub mod arrow_flight_protocol {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/arrow.flight.protocol.rs"));
}

pub mod arrow_flight_protocol_sql {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/arrow.flight.protocol.sql.rs"));
} 