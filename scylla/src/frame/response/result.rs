use anyhow::Result as AResult;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, Bytes};
use std::convert::TryFrom;
use std::net::IpAddr;
use std::str;

use crate::frame::types;

#[derive(Debug)]
pub struct SetKeyspace {
    // TODO
}

#[derive(Debug)]
pub struct Prepared {
    pub id: Bytes,
    pub prepared_metadata: PreparedMetadata,
    result_metadata: ResultMetadata,
}

#[derive(Debug)]
pub struct SchemaChange {
    // TODO
}

#[derive(Clone, Debug)]
struct TableSpec {
    ks_name: String,
    table_name: String,
}

#[derive(Debug, Clone)]
enum ColumnType {
    Ascii,
    Int,
    BigInt,
    Text,
    Inet,
    Set(Box<ColumnType>),
    // TODO
}

#[derive(Debug)]
pub enum CQLValue {
    Ascii(String),
    Int(i32),
    BigInt(i64),
    Text(String),
    Inet(IpAddr),
    Set(Vec<CQLValue>),
    // TODO
}

impl CQLValue {
    pub fn as_ascii(&self) -> Option<&String> {
        match self {
            Self::Ascii(s) => Some(&s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i32> {
        match self {
            Self::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        match self {
            Self::BigInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&String> {
        match self {
            Self::Text(s) => Some(&s),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Self::Ascii(s) => Some(s),
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_inet(&self) -> Option<IpAddr> {
        match self {
            Self::Inet(a) => Some(*a),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&Vec<CQLValue>> {
        match self {
            Self::Set(s) => Some(&s),
            _ => None,
        }
    }

    // TODO
}

// This trait must exist because we can't define From<Option<CqlVal>> for String
// Neither Option nor String are defined in this crate
pub trait FromCQLVal<T> {
    fn from(cql_val: T) -> Self;
}

impl<T: FromCQLVal<CQLValue>> FromCQLVal<Option<CQLValue>> for T {
    fn from(cql_val_opt: Option<CQLValue>) -> Self {
        T::from(cql_val_opt.expect("Tried to convert from CQLValue that is NULL!"))
    }
}

impl<T: FromCQLVal<CQLValue>> FromCQLVal<Option<CQLValue>> for Option<T> {
    fn from(cql_val_opt: Option<CQLValue>) -> Self {
        cql_val_opt.map(T::from)
    }
}

macro_rules! impl_from_cql_val {
    ($T:ty, $convert_func:ident) => {
        impl FromCQLVal<CQLValue> for $T {
            fn from(cql_val: CQLValue) -> $T {
                return cql_val.$convert_func().expect(&format!(
                    "Converting from CQLValue to {} failed!",
                    stringify!($T)
                ));
            }
        }
    };
}

impl_from_cql_val!(i32, as_int); // i32::from<CQLValue>
impl_from_cql_val!(i64, as_bigint); // i64::from<CQLValue>
impl_from_cql_val!(String, into_string); // String::from<CQLValue>

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
                    $($Ti::from(vals_iter.next().expect(&format!("Row is too short to convert to {}!", TUPLE_AS_STR))),)+
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

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    table_spec: TableSpec,
    name: String,
    typ: ColumnType,
}

#[derive(Debug, Default)]
pub struct ResultMetadata {
    col_count: usize,
    pub paging_state: Option<Bytes>,
    col_specs: Vec<ColumnSpec>,
}

#[derive(Debug, Clone)]
pub struct PreparedMetadata {
    pub col_count: usize,
    pub pk_indexes: Vec<u16>,
    pub col_specs: Vec<ColumnSpec>,
}

#[derive(Debug, Default)]
pub struct Row {
    pub columns: Vec<Option<CQLValue>>,
}

#[derive(Debug, Default)]
pub struct Rows {
    pub metadata: ResultMetadata,
    rows_count: usize,
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub enum Result {
    Void,
    Rows(Rows),
    SetKeyspace(SetKeyspace),
    Prepared(Prepared),
    SchemaChange(SchemaChange),
}

fn deser_table_spec(buf: &mut &[u8]) -> AResult<TableSpec> {
    let ks_name = types::read_string(buf)?.to_owned();
    let table_name = types::read_string(buf)?.to_owned();
    Ok(TableSpec {
        ks_name,
        table_name,
    })
}

fn deser_type(buf: &mut &[u8]) -> AResult<ColumnType> {
    use ColumnType::*;
    let id = types::read_short(buf)?;
    Ok(match id {
        0x0001 => Ascii,
        0x0002 => BigInt,
        0x0009 => Int,
        0x000D => Text,
        0x0010 => Inet,
        0x0022 => Set(Box::new(deser_type(buf)?)),
        id => {
            // TODO implement other types
            return Err(anyhow!("Type not yet implemented, id: {}", id));
        }
    })
}

fn deser_col_specs(
    buf: &mut &[u8],
    global_table_spec: &Option<TableSpec>,
    col_count: usize,
) -> AResult<Vec<ColumnSpec>> {
    let mut col_specs = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let table_spec = if let Some(spec) = global_table_spec {
            spec.clone()
        } else {
            deser_table_spec(buf)?
        };
        let name = types::read_string(buf)?.to_owned();
        let typ = deser_type(buf)?;
        col_specs.push(ColumnSpec {
            table_spec,
            name,
            typ,
        });
    }
    Ok(col_specs)
}

fn deser_result_metadata(buf: &mut &[u8]) -> AResult<ResultMetadata> {
    let flags = types::read_int(buf)?;
    let global_tables_spec = flags & 0x0001 != 0;
    let has_more_pages = flags & 0x0002 != 0;
    let no_metadata = flags & 0x0004 != 0;

    let col_count = types::read_int(buf)?;
    if col_count < 0 {
        return Err(anyhow!("Invalid negative column count: {}", col_count));
    }
    let col_count = col_count as usize;

    let paging_state = if has_more_pages {
        Some(types::read_bytes(buf)?.to_owned().into())
    } else {
        None
    };

    if no_metadata {
        return Ok(ResultMetadata {
            col_count,
            paging_state,
            col_specs: vec![],
        });
    }

    let global_table_spec = if global_tables_spec {
        Some(deser_table_spec(buf)?)
    } else {
        None
    };

    let col_specs = deser_col_specs(buf, &global_table_spec, col_count)?;

    Ok(ResultMetadata {
        col_count,
        paging_state,
        col_specs,
    })
}

fn deser_prepared_metadata(buf: &mut &[u8]) -> AResult<PreparedMetadata> {
    let flags = types::read_int(buf)?;
    let global_tables_spec = flags & 0x0001 != 0;

    let col_count = types::read_int(buf)?;
    if col_count < 0 {
        return Err(anyhow!("Invalid negative column count: {}", col_count));
    }
    let col_count = col_count as usize;

    let pk_count = types::read_int(buf)?;
    if pk_count < 0 {
        return Err(anyhow!("Invalid negative pk count: {}", col_count));
    }
    let pk_count = pk_count as usize;

    let mut pk_indexes = Vec::with_capacity(pk_count);
    for _ in 0..pk_count {
        pk_indexes.push(types::read_short(buf)? as u16);
    }

    let global_table_spec = if global_tables_spec {
        Some(deser_table_spec(buf)?)
    } else {
        None
    };

    let col_specs = deser_col_specs(buf, &global_table_spec, col_count)?;

    Ok(PreparedMetadata {
        col_count,
        pk_indexes,
        col_specs,
    })
}

fn deser_cql_value(typ: &ColumnType, buf: &mut &[u8]) -> AResult<CQLValue> {
    use ColumnType::*;
    Ok(match typ {
        Ascii => {
            if !buf.is_ascii() {
                return Err(anyhow!("Not an ascii string: {:?}", buf));
            }
            CQLValue::Ascii(str::from_utf8(buf)?.to_owned())
        }
        Int => {
            if buf.len() != 4 {
                return Err(anyhow!(
                    "Expected buffer length of 4 bytes, got: {}",
                    buf.len()
                ));
            }
            CQLValue::Int(buf.read_i32::<BigEndian>()?)
        }
        BigInt => {
            if buf.len() != 8 {
                return Err(anyhow!(
                    "Expected buffer length of 8 bytes, got: {}",
                    buf.len()
                ));
            }
            CQLValue::BigInt(buf.read_i64::<BigEndian>()?)
        }
        Text => CQLValue::Text(str::from_utf8(buf)?.to_owned()),
        Inet => CQLValue::Inet(match buf.len() {
            4 => {
                let ret = IpAddr::from(<[u8; 4]>::try_from(&buf[0..4])?);
                buf.advance(4);
                ret
            }
            16 => {
                let ret = IpAddr::from(<[u8; 16]>::try_from(&buf[0..16])?);
                buf.advance(16);
                ret
            }
            v => {
                return Err(anyhow!(
                    "Invalid number of bytes for inet value: {}. Expecting 4 or 16",
                    v
                ));
            }
        }),
        Set(typ) => {
            let len = types::read_int(buf)?;
            if len < 0 {
                return Err(anyhow!("Invalid number of set elements: {}", len));
            }
            let mut res = Vec::with_capacity(len as usize);
            for _ in 0..len {
                // TODO: is `null` allowed as set element? Should we use read_bytes_opt?
                let mut b = types::read_bytes(buf)?;
                res.push(deser_cql_value(typ, &mut b)?);
            }
            CQLValue::Set(res)
        }
    })
}

fn deser_rows(buf: &mut &[u8]) -> AResult<Rows> {
    let metadata = deser_result_metadata(buf)?;

    // TODO: the protocol allows an optimization (which must be explicitly requested on query by
    // the driver) where the column metadata is not sent with the result.
    // Implement this optimization. We'll then need to take the column types by a parameter.
    // Beware of races; our column types may be outdated.
    assert!(metadata.col_count == metadata.col_specs.len());

    let rows_count = types::read_int(buf)?;
    if rows_count < 0 {
        return Err(anyhow!("Invalid negative number of rows: {}", rows_count));
    }
    let rows_count = rows_count as usize;

    let mut rows = Vec::with_capacity(rows_count);
    for _ in 0..rows_count {
        let mut columns = Vec::with_capacity(metadata.col_count);
        for i in 0..metadata.col_count {
            let v = if let Some(mut b) = types::read_bytes_opt(buf)? {
                Some(deser_cql_value(&metadata.col_specs[i].typ, &mut b)?)
            } else {
                None
            };
            columns.push(v);
        }
        rows.push(Row { columns: columns });
    }
    Ok(Rows {
        metadata,
        rows_count,
        rows,
    })
}

fn deser_set_keyspace(_buf: &mut &[u8]) -> AResult<SetKeyspace> {
    Ok(SetKeyspace {}) // TODO
}

fn deser_prepared(buf: &mut &[u8]) -> AResult<Prepared> {
    let id_len = types::read_short(buf)? as usize;
    let id: Bytes = buf[0..id_len].to_owned().into();
    buf.advance(id_len);
    let prepared_metadata = deser_prepared_metadata(buf)?;
    let result_metadata = deser_result_metadata(buf)?;
    Ok(Prepared {
        id,
        prepared_metadata,
        result_metadata,
    })
}

fn deser_schema_change(_buf: &mut &[u8]) -> AResult<SchemaChange> {
    Ok(SchemaChange {}) // TODO
}

pub fn deserialize(buf: &mut &[u8]) -> AResult<Result> {
    use self::Result::*;
    Ok(match types::read_int(buf)? {
        0x0001 => Void,
        0x0002 => Rows(deser_rows(buf)?),
        0x0003 => SetKeyspace(deser_set_keyspace(buf)?),
        0x0004 => Prepared(deser_prepared(buf)?),
        0x0005 => SchemaChange(deser_schema_change(buf)?),
        k => return Err(anyhow!("Unknown query result kind: {}", k)),
    })
}
