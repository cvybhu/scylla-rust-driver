use crate::frame::frame_errors::{FrameError, ParseError};
use crate::frame::value::SerializeValuesError;
use crate::statement::Consistency;
use std::sync::Arc;
use thiserror::Error;

/// Error that occured during query execution
#[derive(Error, Debug, Clone)]
pub enum QueryError {
    /// Database sent a response containing some error with a message
    #[error("Database returned an error: {0}, Error message: {1}")]
    DBError(DBError, String),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Input/Output error has occured, connection broken etc.
    #[error("IO Error: {0}")]
    IOError(Arc<std::io::Error>),

    /// Unexpected or invalid message received
    #[error("Protocol Error: {0}")]
    ProtocolError(&'static str),
}

/// An error sent from the database in response to a query
/// as described in the [specification](https://github.com/apache/cassandra/blob/5ed5e84613ef0e9664a774493db7d2604e3596e0/doc/native_protocol_v4.spec#L1029)  
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum DBError {
    /// The submitted query has a syntax error
    #[error("The submitted query has a syntax error")]
    SyntaxError,

    /// The query is syntatically correct but invalid
    #[error("The query is syntatically correct but invalid")]
    Invalid,

    /// Attempted to create a keyspace or a table that was already existing
    #[error("Attempted to create a keyspace or a table that was already existing")]
    AlreadyExists {
        /// Created keyspace name or name of the keyspace in which table was created
        keyspace: String,
        /// Name of the table created, in case of keyspace creation it's an empty string
        table: String,
    },

    /// User defined function failed during execution
    #[error("User defined function failed during execution")]
    FunctionFailure {
        /// Keyspace of the failed function
        keyspace: String,
        /// Name of the failed function
        function: String,
        /// Types of arguments passed to the function
        arg_types: Vec<String>,
    },

    /// Authentication failed - bad credentials
    #[error("Authentication failed - bad credentials")]
    AuthenticationError,

    /// The logged user doesn't have the right to perform the query
    #[error("The logged user doesn't have the right to perform the query")]
    Unauthorized,

    /// The query is invalid because of some configuration issue
    #[error("The query is invalid because of some configuration issue")]
    ConfigError,

    /// Not enough nodes are alive to satisfy required consistency level
    #[error("Not enough nodes are alive to satisfy required consistency level")]
    Unavailable {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes required to be alive to satisfy required consistency level
        required: i32,
        /// Found number of active nodes
        alive: i32,
    },

    /// The request cannot be processed because the coordinator node is overloaded
    #[error("The request cannot be processed because the coordinator node is overloaded")]
    Overloaded,

    /// The coordinator node is still bootstrapping
    #[error("The coordinator node is still bootstrapping")]
    IsBootstrapping,

    /// Error during truncate operation
    #[error("Error during truncate operation")]
    TruncateError,

    /// Not enough nodes responded to the read request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the read request in time to satisfy required consistency level")]
    ReadTimeout {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// Not enough nodes responded to the write request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the write request in time to satisfy required consistency level")]
    WriteTimeout {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes that responded to the write request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// A non-timeout error during a read request
    #[error("A non-timeout error during a read request")]
    ReadFailure {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of nodes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Number of nodes that experience a failure while executing the request
        numfailures: i32,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// A non-timeout error during a write request
    #[error("A non-timeout error during a write request")]
    WriteFailure {
        /// Consistency level of the query
        consistency: Consistency,
        /// Number of ndoes that responded to the read request
        received: i32,
        /// Number of nodes required to respond to satisfy required consistency level
        required: i32,
        /// Number of nodes that experience a failure while executing the request
        numfailures: i32,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// Tried to execute a prepared statement that is not prepared. Driver shoud prepare it again
    #[error(
        "Tried to execute a prepared statement that is not prepared. Driver shoud prepare it again"
    )]
    Unprepared,

    /// Internal server error. This indicates a server-side bug
    #[error("Internal server error. This indicates a server-side bug")]
    ServerError,

    /// Invalid protocol message received from the driver
    #[error("Invalid protocol message received from the driver")]
    ProtocolError,

    /// Other error code not specified in the specification
    #[error("Other error not specified in the specification. Error code: {0}")]
    Other(i32),
}

// Type of write operation requested
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteType {
    /// Non-batched non-counter write
    Simple,
    /// Logged batch write. If this type is received, it means the batch log has been succesfully written
    /// (otherwise BatchLog type would be present)
    Batch,
    /// Unlogged batch. No batch log write has been attempted.
    UnloggedBatch,
    /// Counter write (batched or not)
    Counter,
    /// Timeout occured during the write to the batch log when a logged batch was requested
    BatchLog,
    /// Timeout occured during Compare And Set write/update
    CAS,
    /// Write involves VIEW update and failure to acquire local view(MV) lock for key within timeout
    View,
    /// Timeout occured  when a cdc_total_space_in_mb is exceeded when doing a write to data tracked by cdc
    CDC,
    /// Other type not specified in the specification
    Other(String),
}

/// Error caused by caller creating an invalid query
#[derive(Error, Debug, Clone)]
#[error("Invalid query passed to Session")]
pub enum BadQuery {
    /// Failed to serialize values passed to a query - values too big
    #[error("Serializing values failed: {0} ")]
    SerializeValuesError(#[from] SerializeValuesError),

    /// Number of values provided doesn't match number of statements in a batch
    #[error("Length of provided values ({0}) must be equal to number of batch statements ({1})")]
    ValueLenMismatch(usize, usize),

    /// Serialized values are too long to compute parition key
    #[error("Serialized values are too long to compute parition key! Length: {0}, Max allowed length: {1}")]
    ValuesTooLongForKey(usize, usize),

    /// Passed invalid keyspace name to use
    #[error("Passed invalid keyspace name to use: {0}")]
    BadKeyspaceName(#[from] BadKeyspaceName),
}

/// Error that occured during session creation
#[derive(Error, Debug, Clone)]
pub enum NewSessionError {
    /// Failed to resolve hostname passed in Session creation
    #[error("Couldn't resolve address: {0}")]
    FailedToResolveAddress(String),

    /// List of known nodes passed to Session constructor is empty
    /// There needs to be at least one node to connect to
    #[error("Empty known nodes list")]
    EmptyKnownNodesList,

    /// Database sent a response containing some error with a message
    #[error("Database returned an error: {0}, Error message: {1}")]
    DBError(DBError, String),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Input/Output error has occured, connection broken etc.
    #[error("IO Error: {0}")]
    IOError(Arc<std::io::Error>),

    /// Unexpected or invalid message received
    #[error("Protocol Error: {0}")]
    ProtocolError(&'static str),
}

/// Invalid keyspace name given to `Session::use_keyspace()`
#[derive(Debug, Error, Clone)]
pub enum BadKeyspaceName {
    /// Keyspace name is empty
    #[error("Keyspace name is empty")]
    Empty,

    /// Keyspace name too long, must be up to 48 characters
    #[error("Keyspace name too long, must be up to 48 characters, found {1} characters. Bad keyspace name: '{0}'")]
    TooLong(String, usize),

    /// Illegal character - only alpha-numeric and underscores allowed.
    #[error("Illegal character found: '{1}', only alpha-numeric and underscores allowed. Bad keyspace name: '{0}'")]
    IllegalCharacter(String, char),
}

impl From<std::io::Error> for QueryError {
    fn from(io_error: std::io::Error) -> QueryError {
        QueryError::IOError(Arc::new(io_error))
    }
}

impl From<SerializeValuesError> for QueryError {
    fn from(serialized_err: SerializeValuesError) -> QueryError {
        QueryError::BadQuery(BadQuery::SerializeValuesError(serialized_err))
    }
}

impl From<ParseError> for QueryError {
    fn from(_parse_error: ParseError) -> QueryError {
        QueryError::ProtocolError("Error parsing message")
    }
}

impl From<FrameError> for QueryError {
    fn from(_frame_error: FrameError) -> QueryError {
        QueryError::ProtocolError("Error parsing message frame")
    }
}

impl From<std::io::Error> for NewSessionError {
    fn from(io_error: std::io::Error) -> NewSessionError {
        NewSessionError::IOError(Arc::new(io_error))
    }
}

impl From<QueryError> for NewSessionError {
    fn from(query_error: QueryError) -> NewSessionError {
        match query_error {
            QueryError::DBError(e, msg) => NewSessionError::DBError(e, msg),
            QueryError::BadQuery(e) => NewSessionError::BadQuery(e),
            QueryError::IOError(e) => NewSessionError::IOError(e),
            QueryError::ProtocolError(m) => NewSessionError::ProtocolError(m),
        }
    }
}

impl From<BadKeyspaceName> for QueryError {
    fn from(keyspace_err: BadKeyspaceName) -> QueryError {
        QueryError::BadQuery(BadQuery::BadKeyspaceName(keyspace_err))
    }
}

impl From<&str> for WriteType {
    fn from(write_type_str: &str) -> WriteType {
        match write_type_str {
            "SIMPLE" => WriteType::Simple,
            "BATCH" => WriteType::Batch,
            "UNLOGGED_BATCH" => WriteType::UnloggedBatch,
            "COUNTER" => WriteType::Counter,
            "BATCH_LOG" => WriteType::BatchLog,
            "CAS" => WriteType::CAS,
            "VIEW" => WriteType::View,
            "CDC" => WriteType::CDC,
            _ => WriteType::Other(write_type_str.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WriteType;

    #[test]
    fn write_type_from_str() {
        let test_cases: [(&str, WriteType); 9] = [
            ("SIMPLE", WriteType::Simple),
            ("BATCH", WriteType::Batch),
            ("UNLOGGED_BATCH", WriteType::UnloggedBatch),
            ("COUNTER", WriteType::Counter),
            ("BATCH_LOG", WriteType::BatchLog),
            ("CAS", WriteType::CAS),
            ("VIEW", WriteType::View),
            ("CDC", WriteType::CDC),
            ("SOMEOTHER", WriteType::Other("SOMEOTHER".to_string())),
        ];

        for (write_type_str, expected_write_type) in &test_cases {
            let write_type = WriteType::from(*write_type_str);
            assert_eq!(write_type, *expected_write_type);
        }
    }
}
