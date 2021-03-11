use std::collections::HashMap;
use std::net::IpAddr;
use uuid::Uuid;

use crate::cql_to_rust::{FromRow, FromRowError};
use crate::frame::response::result::Row;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracingInfo {
    pub client: IpAddr,
    pub command: String,
    pub coordinator: IpAddr,
    pub duration: i32,
    pub parameters: HashMap<String, String>,
    pub request: String,
    pub started_at: i64, // TODO: Change to timestamp

    pub events: Vec<TracingEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracingEvent {
    event_id: Uuid,
    activity: String,
    source: IpAddr,
    source_elapsed: i32,
    thread: String,
}

// A query used to query TracingInfo from system_traces.sessions
pub(crate) const TRACES_SESSION_QUERY_STR: &str =
    "SELECT client, command, coordinator, duration, parameters, request, started_at \
    FROM system_traces.sessions WHERE session_id = ?";

// A query used to query TracingEvent from system_traces.events
pub(crate) const TRACES_EVENTS_QUERY_STR: &str =
    "SELECT event_id, activity, source, source_elapsed, thread \
    FROM system_traces.events WHERE session_id = ?";

// Converts a row received by performing TRACES_SESSION_QUERY_STR to TracingInfo
impl FromRow for TracingInfo {
    fn from_row(row: Row) -> Result<TracingInfo, FromRowError> {
        let (client, command, coordinator, duration, parameters, request, started_at) =
            <(
                IpAddr,
                String,
                IpAddr,
                i32,
                HashMap<String, String>,
                String,
                i64,
            )>::from_row(row)?;

        Ok(TracingInfo {
            client,
            command,
            coordinator,
            duration,
            parameters,
            request,
            started_at,
            events: Vec::new(),
        })
    }
}

impl FromRow for TracingEvent {
    fn from_row(row: Row) -> Result<TracingEvent, FromRowError> {
        let (event_id, activity, source, source_elapsed, thread) =
            <(Uuid, String, IpAddr, i32, String)>::from_row(row)?;

        Ok(TracingEvent {
            event_id,
            activity,
            source,
            source_elapsed,
            thread,
        })
    }
}
