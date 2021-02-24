use super::Consistency;
use crate::transport::retry_policy::RetryPolicy;

/// CQL query statement.
///
/// This represents a CQL query that can be executed on a server.
pub struct Query {
    contents: String,
    page_size: Option<i32>,
    pub consistency: Consistency,
    pub is_idempotent: bool,
    pub retry_policy: Option<Box<dyn RetryPolicy + Send + Sync>>,
}

impl Query {
    /// Creates a new `Query` from a CQL query string.
    pub fn new(contents: String) -> Self {
        Self {
            contents,
            page_size: None,
            consistency: Default::default(),
            is_idempotent: false,
            retry_policy: None,
        }
    }

    /// Returns the string representation of the CQL query.
    pub fn get_contents(&self) -> &str {
        &self.contents
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

    /// Sets the consistency to be used when executing this query.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.consistency = c;
    }

    /// Gets the consistency to be used when executing this query.
    pub fn get_consistency(&self) -> Consistency {
        self.consistency
    }
}

impl From<String> for Query {
    fn from(s: String) -> Query {
        Query::new(s)
    }
}

impl<'a> From<&'a str> for Query {
    fn from(s: &'a str) -> Query {
        Query::new(s.to_owned())
    }
}

impl Clone for Query {
    fn clone(&self) -> Query {
        Query {
            contents: self.contents.clone(),
            page_size: self.page_size,
            consistency: self.consistency,
            is_idempotent: self.is_idempotent,
            retry_policy: self.retry_policy.as_ref().map(|rp| rp.clone_boxed()),
        }
    }
}
