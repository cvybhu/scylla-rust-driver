use crate::statement::Consistency;
use crate::transport::errors::{DBError, QueryError, WriteType};

/// Information about a failed query
pub struct QueryInfo {
    /// The error with which the query failed
    pub error: QueryError,
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application  
    /// If set to `true` we can be sure that it is idempotent  
    /// If set to `false` it is unknown whether it is idempotent
    pub is_idempotent: bool,
    /// Consistency with which the query failed
    pub consistency: Consistency,
}

pub enum RetryDecision {
    RetrySameNode,
    RetryNextNode,
    DontRetry,
}

pub trait RetryPolicy {
    fn decide_should_retry(&mut self, query_info: QueryInfo) -> RetryDecision;
}

/// Forwards all errors directly to the user, never retries
pub struct FallthroughRetryPolicy;

impl RetryPolicy for FallthroughRetryPolicy {
    fn decide_should_retry(&mut self, _query_info: QueryInfo) -> RetryDecision {
        RetryDecision::DontRetry
    }
}

/// Default retry policy - retries when there is a high chance that a retry might help.  
/// Behaviour based on [DataStax Java Driver](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/retries/)
pub struct DefaultRetryPolicy {
    was_unavailable_retry: bool,
    was_read_timeout_retry: bool,
}

impl DefaultRetryPolicy {
    pub fn new() -> DefaultRetryPolicy {
        DefaultRetryPolicy {
            was_unavailable_retry: false,
            was_read_timeout_retry: false,
        }
    }
}

impl RetryPolicy for DefaultRetryPolicy {
    fn decide_should_retry(&mut self, query_info: QueryInfo) -> RetryDecision {
        match query_info.error {
            // Basic errors - there are some problems on this node
            // Retry on a different one if possible
            QueryError::IOError(_)
            | QueryError::DBError(DBError::Overloaded, _)
            | QueryError::DBError(DBError::ServerError, _)
            | QueryError::DBError(DBError::TruncateError, _) => {
                if query_info.is_idempotent {
                    RetryDecision::RetryNextNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Unavailable - the current node believes that not enough nodes
            // are alive to satisfy specified consistency requirements.
            // Maybe this node has network problems - try a different one.
            // Perform at most one retry  - it's unlikely that two nodes
            // have network problems at the same time
            QueryError::DBError(DBError::Unavailable { .. }, _) => {
                if !self.was_unavailable_retry {
                    self.was_unavailable_retry = true;
                    RetryDecision::RetryNextNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            // ReadTimeout - coordinator didn't receive enough replies in time.
            // Retry at most once and only if there were actually enough replies
            // to satisfy consistency but they were all just checksums (data_present == true).
            // This happens when the coordinator picked replicas that were overloaded/dying.
            // Retried request should have some useful response because the node will detect
            // that these replicas are dead.
            QueryError::DBError(
                DBError::ReadTimeout {
                    received,
                    required,
                    data_present,
                    ..
                },
                _,
            ) => {
                if !self.was_read_timeout_retry && received >= required && data_present {
                    self.was_read_timeout_retry = true;
                    RetryDecision::RetrySameNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            // Write timeout - coordinator didn't receive enough replies in time.
            // Retry at most once and only for BatchLog write.
            // Coordinator probably didn't detect the nodes as dead.
            // By the time we retry they should be detected as dead.
            QueryError::DBError(DBError::WriteTimeout { write_type, .. }, _) => {
                if query_info.is_idempotent && write_type == WriteType::BatchLog {
                    RetryDecision::RetrySameNode
                } else {
                    RetryDecision::DontRetry
                }
            }
            // The node is still bootstrapping it can't execute the query, we should try another one
            QueryError::DBError(DBError::IsBootstrapping, _) => RetryDecision::RetryNextNode,
            // In all other cases propagate the error to the user
            _ => RetryDecision::DontRetry,
        }
    }
}

impl Default for DefaultRetryPolicy {
    fn default() -> DefaultRetryPolicy {
        DefaultRetryPolicy::new()
    }
}
