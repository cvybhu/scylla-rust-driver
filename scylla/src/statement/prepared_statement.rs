use super::Consistency;
use crate::frame::response::result::PreparedMetadata;
use crate::frame::value::SerializedValues;
use crate::transport::retry_policy::RetryPolicy;
use bytes::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;
use thiserror::Error;

/// Represents a statement prepared on the server.
pub struct PreparedStatement {
    id: Bytes,
    metadata: PreparedMetadata,
    statement: String,
    page_size: Option<i32>,
    pub consistency: Consistency,
    pub is_idempotent: bool,
    pub retry_policy: Option<Box<dyn RetryPolicy + Send + Sync>>,
    pub tracing: bool,
}

impl PreparedStatement {
    pub fn new(id: Bytes, metadata: PreparedMetadata, statement: String) -> Self {
        Self {
            id,
            metadata,
            statement,
            page_size: None,
            consistency: Default::default(),
            is_idempotent: false,
            retry_policy: None,
            tracing: false,
        }
    }

    pub fn get_id(&self) -> &Bytes {
        &self.id
    }

    pub fn get_statement(&self) -> &str {
        &self.statement
    }

    /// Sets the page size for this CQL query.
    pub fn set_page_size(&mut self, page_size: i32) {
        assert!(page_size > 0, "page size must be larger than 0");
        self.page_size = Some(page_size);
    }

    /// Disables paging for this CQL query.
    pub fn disable_paging(&mut self) {
        self.page_size = None;
    }

    /// Returns the page size for this CQL query.
    pub fn get_page_size(&self) -> Option<i32> {
        self.page_size
    }

    /// Sets the consistency to be used when executing this statement.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.consistency = c;
    }

    /// Gets the consistency to be used when executing this statement.
    pub fn get_consistency(&self) -> Consistency {
        self.consistency
    }

    /// Sets the idempotence of this statement  
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application  
    /// If set to `true` we can be sure that it is idempotent  
    /// If set to `false` it is unknown whether it is idempotent  
    /// This is used in [`RetryPolicy`] to decide if retrying a query is safe
    pub fn set_is_idempotent(&mut self, is_idempotent: bool) {
        self.is_idempotent = is_idempotent;
    }

    /// Gets the idempotence of this statement
    pub fn get_is_idempotent(&self) -> bool {
        self.is_idempotent
    }

    /// Sets a custom [`RetryPolicy`] to be used with this statement  
    /// By default Session's retry policy is used, this allows to use a custom retry policy
    pub fn set_retry_policy(&mut self, retry_policy: Box<dyn RetryPolicy + Send + Sync>) {
        self.retry_policy = Some(retry_policy);
    }

    /// Gets custom [`RetryPolicy`] used by this statement
    pub fn get_retry_policy(&self) -> &Option<Box<dyn RetryPolicy + Send + Sync>> {
        &self.retry_policy
    }

    /// Enable or disable CQL Tracing for this prepared statemnt  
    /// If enabled session.execute() will return QueryResult containing tracing_id
    /// which can be used to query tracing information about the execution of this statement
    pub fn set_tracing(&mut self, should_trace: bool) {
        self.tracing = should_trace;
    }

    /// Gets whether tracing is enabled for this prepared statement
    pub fn get_tracing(&self) -> bool {
        self.tracing
    }

    /// Computes the partition key of the target table from given values
    /// Partition keys have a specific serialization rules.
    /// Ref: https://github.com/scylladb/scylla/blob/40adf38915b6d8f5314c621a94d694d172360833/compound_compat.hh#L33-L47
    pub fn compute_partition_key(
        &self,
        bound_values: &SerializedValues,
    ) -> Result<Bytes, PartitionKeyError> {
        let mut buf = BytesMut::new();

        if self.metadata.pk_indexes.len() == 1 {
            if let Some(v) = bound_values
                .iter()
                .nth(self.metadata.pk_indexes[0] as usize)
                .ok_or_else(|| {
                    PartitionKeyError::NoPkIndexValue(
                        self.metadata.pk_indexes[0],
                        bound_values.len(),
                    )
                })?
            {
                buf.extend_from_slice(v);
            }
            return Ok(buf.into());
        }
        // TODO: consider what happens if a prepared statement is of type (?, something, ?),
        // where all three parameters form a partition key. The middle one is not available
        // in bound values.

        // TODO: Optimize - maybe we could check if pk_indexes are sorted and do an allocation-free two-pointer sweep algorithm then?
        // We can't just sort them because the hash will break:
        // https://github.com/apache/cassandra/blob/caeecf6456b87886a79f47a2954788e6c856697c/doc/native_protocol_v4.spec#L673

        let values: Vec<Option<&[u8]>> = bound_values.iter().collect();
        for pk_index in &self.metadata.pk_indexes {
            // Find value matching current pk_index
            let next_val: &Option<&[u8]> = values
                .get(*pk_index as usize)
                .ok_or_else(|| PartitionKeyError::NoPkIndexValue(*pk_index, bound_values.len()))?;

            // Add value's bytes
            if let Some(v) = next_val {
                let v_len_u16: u16 = v
                    .len()
                    .try_into()
                    .map_err(|_| PartitionKeyError::ValueTooLong(v.len()))?;

                buf.put_u16(v_len_u16);
                buf.extend_from_slice(v);
                buf.put_u8(0);
            }
        }

        Ok(buf.into())
    }

    /// Returns the name of the keyspace this statement is operating on.
    pub fn get_keyspace_name(&self) -> Option<&str> {
        self.metadata
            .col_specs
            .first()
            .map(|col_spec| col_spec.table_spec.ks_name.as_str())
    }
}

#[derive(Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKeyError {
    #[error("No value with given pk_index! pk_index: {0}, values.len(): {1}")]
    NoPkIndexValue(u16, i16),
    #[error("Value bytes too long to create partition key, max 65 535 allowed! value.len(): {0}")]
    ValueTooLong(usize),
}

impl Clone for PreparedStatement {
    fn clone(&self) -> PreparedStatement {
        PreparedStatement {
            id: self.id.clone(),
            metadata: self.metadata.clone(),
            statement: self.statement.clone(),
            page_size: self.page_size,
            consistency: self.consistency,
            is_idempotent: self.is_idempotent,
            retry_policy: self
                .retry_policy
                .as_ref()
                .map(|policy| policy.clone_boxed()),
            tracing: self.tracing,
        }
    }
}
