use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::retry_policy::RetryPolicy;
use std::sync::Arc;

pub use super::Consistency;
pub use crate::frame::request::batch::BatchType;

/// CQL batch statement.
///
/// This represents a CQL batch that can be executed on a server.
#[derive(Clone)]
pub struct Batch {
    statements: Vec<BatchStatement>,
    batch_type: BatchType,
    pub consistency: Consistency,
    pub is_idempotent: bool,
    pub retry_policy: Option<Arc<dyn RetryPolicy + Send + Sync>>,
}

impl Batch {
    /// Creates a new, empty `Batch` of `batch_type` type.
    pub fn new(batch_type: BatchType) -> Self {
        Self {
            statements: Vec::new(),
            batch_type,
            consistency: Default::default(),
            is_idempotent: false,
            retry_policy: None,
        }
    }

    /// Appends a new statement to the batch.
    pub fn append_statement(&mut self, statement: impl Into<BatchStatement>) {
        self.statements.push(statement.into());
    }

    /// Gets type of batch.
    pub fn get_type(&self) -> BatchType {
        self.batch_type
    }

    /// Returns statements contained in the batch.
    pub fn get_statements(&self) -> &[BatchStatement] {
        self.statements.as_ref()
    }

    /// Sets the consistency to be used when executing this batch.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.consistency = c;
    }

    /// Gets the consistency to be used when executing this batch.
    pub fn get_consistency(&self) -> Consistency {
        self.consistency
    }
}

impl Default for Batch {
    fn default() -> Self {
        Batch::new(BatchType::Logged)
    }
}

/// This enum represents a CQL statement, that can be part of batch.
#[derive(Clone)]
pub enum BatchStatement {
    Query(Query),
    PreparedStatement(PreparedStatement),
}

impl From<&str> for BatchStatement {
    fn from(s: &str) -> Self {
        BatchStatement::Query(Query::from(s))
    }
}

impl From<Query> for BatchStatement {
    fn from(q: Query) -> Self {
        BatchStatement::Query(q)
    }
}

impl From<PreparedStatement> for BatchStatement {
    fn from(p: PreparedStatement) -> Self {
        BatchStatement::PreparedStatement(p)
    }
}
