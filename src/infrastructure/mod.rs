//! Infrastructure layer — I/O adapters. Depends on `domain`; converts external
//! representations (protobuf, NTP wire format, TOML) to/from domain types.

pub mod config_file;
pub mod geyser;
pub mod proto;
pub mod sntp;
