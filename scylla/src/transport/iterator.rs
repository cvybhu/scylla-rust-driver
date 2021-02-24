use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::Stream;
use std::result::Result as StdResult;
use thiserror::Error;
use tokio::sync::mpsc;

use super::errors::QueryError;
use crate::cql_to_rust::{FromRow, FromRowError};

use crate::frame::{
    response::{
        result::{Result, Row, Rows},
        Response,
    },
    value::SerializedValues,
};
use crate::routing::Token;
use crate::statement::Consistency;
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::cluster::ClusterData;
use crate::transport::connection::Connection;
use crate::transport::load_balancing::{LoadBalancingPolicy, Statement};
use crate::transport::metrics::Metrics;
use crate::transport::retry_policy::{QueryInfo, RetryDecision, RetryPolicy};

pub struct RowIterator {
    current_row_idx: usize,
    current_page: Rows,
    page_receiver: mpsc::Receiver<StdResult<Rows, QueryError>>,
}

impl Stream for RowIterator {
    type Item = StdResult<Row, QueryError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        if s.is_current_page_exhausted() {
            match Pin::new(&mut s.page_receiver).poll_recv(cx) {
                Poll::Ready(Some(Ok(rows))) => {
                    s.current_page = rows;
                    s.current_row_idx = 0;
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }

        let idx = s.current_row_idx;
        if idx < s.current_page.rows.len() {
            let row = mem::take(&mut s.current_page.rows[idx]);
            s.current_row_idx += 1;
            return Poll::Ready(Some(Ok(row)));
        }

        // We probably got a zero-sized page
        // Yield, but tell that we are ready
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RowIterator {
    pub fn into_typed<RowT: FromRow>(self) -> TypedRowIterator<RowT> {
        TypedRowIterator {
            row_iterator: self,
            phantom_data: Default::default(),
        }
    }

    pub(crate) fn new_for_query(
        query: Query,
        values: SerializedValues,
        cluster_data: Arc<ClusterData>,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        load_balancer: Arc<dyn LoadBalancingPolicy>,
        metrics: Arc<Metrics>,
    ) -> RowIterator {
        let (sender, receiver) = mpsc::channel(1);

        let worker = WorkerHelper {
            sender,
            metrics,
            load_balancer,
            retry_policy: retry_policy.clone_boxed(),
            retry_policy_untouched: retry_policy,
            query_idempotence: query.is_idempotent,
            query_consistency: query.consistency,
            page_query: PageQuery::Simple { query, values },
            paging_state: None,
        };

        tokio::task::spawn(worker.work(cluster_data));

        RowIterator {
            current_row_idx: 0,
            current_page: Default::default(),
            page_receiver: receiver,
        }
    }

    pub(crate) fn new_for_prepared_statement(
        prepared: PreparedStatement,
        values: SerializedValues,
        token: Token,
        cluster_data: Arc<ClusterData>,
        retry_policy: Box<dyn RetryPolicy + Send + Sync>,
        load_balancer: Arc<dyn LoadBalancingPolicy>,
        metrics: Arc<Metrics>,
    ) -> RowIterator {
        let (sender, receiver) = mpsc::channel(1);

        let worker = WorkerHelper {
            sender,
            metrics,
            load_balancer,
            retry_policy: retry_policy.clone_boxed(),
            retry_policy_untouched: retry_policy,
            query_idempotence: prepared.is_idempotent,
            query_consistency: prepared.consistency,
            page_query: PageQuery::Prepared {
                prepared,
                values,
                token,
            },
            paging_state: None,
        };

        tokio::task::spawn(worker.work(cluster_data));

        RowIterator {
            current_row_idx: 0,
            current_page: Default::default(),
            page_receiver: receiver,
        }
    }

    fn is_current_page_exhausted(&self) -> bool {
        self.current_row_idx >= self.current_page.rows.len()
    }
}

struct WorkerHelper {
    sender: mpsc::Sender<StdResult<Rows, QueryError>>,
    metrics: Arc<Metrics>,
    load_balancer: Arc<dyn LoadBalancingPolicy>,
    retry_policy: Box<dyn RetryPolicy + Send + Sync>,
    retry_policy_untouched: Box<dyn RetryPolicy + Send + Sync>,
    paging_state: Option<Bytes>,
    page_query: PageQuery,
    query_idempotence: bool,
    query_consistency: Consistency,
}

enum PageQuery {
    Simple {
        query: Query,
        values: SerializedValues,
    },
    Prepared {
        prepared: PreparedStatement,
        values: SerializedValues,
        token: Token,
    },
}

impl WorkerHelper {
    async fn work(mut self, cluster_data: Arc<ClusterData>) {
        let statement_info = Statement {
            token: None,
            keyspace: None,
        };
        let query_plan = self.load_balancer.plan(&statement_info, &cluster_data);

        let mut last_error: QueryError =
            QueryError::ProtocolError("Empty query plan - driver bug!");

        'nodes_in_plan: for node in query_plan {
            let get_connection_res = match &self.page_query {
                PageQuery::Simple { .. } => node.random_connection().await,
                PageQuery::Prepared { token, .. } => node.connection_for_token_or_any(*token).await,
            };

            let connection: Arc<Connection> = match get_connection_res {
                Ok(conn) => conn,
                Err(e) => {
                    last_error = e; // All connections broken, try another node
                    continue 'nodes_in_plan;
                }
            };

            'same_node_retries: loop {
                // Now do queries unitl an error or success
                let queries_result = self.query_pages(&connection).await;

                last_error = match queries_result {
                    Ok(_) => return, // All pages fetched succesfully, can stop working
                    Err(e) => e,
                };
                self.metrics.inc_failed_paged_queries();

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: &last_error,
                    is_idempotent: self.query_idempotence,
                    consistency: self.query_consistency,
                };

                match self.retry_policy.decide_should_retry(query_info) {
                    RetryDecision::RetrySameNode => continue 'same_node_retries,
                    RetryDecision::RetryNextNode => continue 'nodes_in_plan,
                    RetryDecision::DontRetry => break 'same_node_retries,
                };
            }
        }

        // Send last_error to user - query failed fully
        let _ = self.sender.send(Err(last_error));
    }

    async fn query_pages(&mut self, connection: &Connection) -> StdResult<(), QueryError> {
        loop {
            let now = Instant::now();
            self.metrics.inc_total_paged_queries();

            let response: Response = match &self.page_query {
                PageQuery::Simple { query, values } => {
                    connection
                        .query(&query, &values, self.paging_state.clone())
                        .await?
                }
                PageQuery::Prepared {
                    prepared, values, ..
                } => {
                    connection
                        .execute_raw_response(prepared, values, self.paging_state.clone())
                        .await?
                }
            };

            let _ = self
                .metrics
                .log_query_latency(now.elapsed().as_millis() as u64);

            match response {
                Response::Result(Result::Rows(mut rows)) => {
                    self.paging_state = rows.metadata.paging_state.take();

                    if self.sender.send(Ok(rows)).await.is_err() {
                        // TODO: Log error
                        // TODO: WTF is going on here?
                    }

                    self.retry_policy = self.retry_policy_untouched.clone_boxed();
                }
                _ => {
                    return Err(QueryError::ProtocolError(
                        "Unexpected response to next page query",
                    ))
                }
            }
        }
    }
}

pub struct TypedRowIterator<RowT> {
    row_iterator: RowIterator,
    phantom_data: std::marker::PhantomData<RowT>,
}

/// Couldn't get next typed row from the iterator
#[derive(Error, Debug, Clone)]
pub enum NextRowError {
    /// Query to fetch next page has failed
    #[error(transparent)]
    QueryError(#[from] QueryError),

    /// Parsing values in row as given types failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

impl<RowT: FromRow> Stream for TypedRowIterator<RowT> {
    type Item = StdResult<RowT, NextRowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        let next_elem: Option<StdResult<Row, QueryError>> =
            match Pin::new(&mut s.row_iterator).poll_next(cx) {
                Poll::Ready(next_elem) => next_elem,
                Poll::Pending => return Poll::Pending,
            };

        let next_ready: Option<Self::Item> = match next_elem {
            Some(Ok(next_row)) => Some(RowT::from_row(next_row).map_err(|e| e.into())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        };

        Poll::Ready(next_ready)
    }
}

// TypedRowIterator can be moved freely for any RowT so it's Unpin
impl<RowT> Unpin for TypedRowIterator<RowT> {}
