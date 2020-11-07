use super::result::{CQLValue, Row};
use std::net::IpAddr;

// This trait must exist because we can't define From<Option<CqlVal>> for String
// Neither Option nor String are defined in this crate
pub trait FromCQLVal<T> {
    fn from_cql(cql_val: T) -> Self;
}

impl<T: FromCQLVal<CQLValue>> FromCQLVal<Option<CQLValue>> for T {
    fn from_cql(cql_val_opt: Option<CQLValue>) -> Self {
        T::from_cql(cql_val_opt.expect("Tried to convert from CQLValue that is NULL!"))
    }
}

impl<T: FromCQLVal<CQLValue>> FromCQLVal<Option<CQLValue>> for Option<T> {
    fn from_cql(cql_val_opt: Option<CQLValue>) -> Self {
        cql_val_opt.map(T::from_cql)
    }
}

macro_rules! impl_from_cql_val {
    ($T:ty, $convert_func:ident) => {
        impl FromCQLVal<CQLValue> for $T {
            fn from_cql(cql_val: CQLValue) -> $T {
                return cql_val.$convert_func().unwrap_or_else(|| {
                    panic!("Converting from CQLValue to {} failed!", stringify!($T))
                });
            }
        }
    };
}

impl_from_cql_val!(i32, as_int); // i32::from_cql<CQLValue>
impl_from_cql_val!(i64, as_bigint); // i64::from_cql<CQLValue>
impl_from_cql_val!(String, into_string); // String::from_cql<CQLValue>
impl_from_cql_val!(IpAddr, as_inet); // IpAddr::from_cql<CQLValue>

// Vec<T>::from_cql<CQLValue>
impl<T: FromCQLVal<CQLValue>> FromCQLVal<CQLValue> for Vec<T> {
    fn from_cql(cql_val: CQLValue) -> Self {
        cql_val
            .into_set()
            .expect("Converting from CQLValue to Vec<T> failed!")
            .into_iter()
            .map(|cql_val| T::from_cql(cql_val))
            .collect()
    }
}

macro_rules! impl_tuple_from_row {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> From<Row> for ($($Ti,)+)
        where
            $($Ti: FromCQLVal<Option<CQLValue>>),+
        {
            fn from(row: Row) -> Self {
                let mut vals_iter = row.columns.into_iter();
                const TUPLE_AS_STR: &'static str = stringify!(($($Ti,)+));

                (
                    $($Ti::from_cql(vals_iter.next().expect(&format!("Row is too short to convert to {}!", TUPLE_AS_STR))),)+
                )
            }
        }
    }
}

impl_tuple_from_row!(T1);
impl_tuple_from_row!(T1, T2);
impl_tuple_from_row!(T1, T2, T3);
impl_tuple_from_row!(T1, T2, T4, T5);
impl_tuple_from_row!(T1, T2, T4, T5, T6);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_from_row!(T1, T2, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
