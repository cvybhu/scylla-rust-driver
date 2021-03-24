# Data Types

The driver maps data types from the database to matching Rust types
to achieve seamless sending and receiving of CQL values.

Mappings:
* `Boolean` <----> `bool`
* `Tinyint`  <---->  `i8`
* `Smallint` <----> `i16`
* `Int` <----> `i32`
* `BigInt` <----> `i64`
* `Float` <----> `f32`
* `Double` <----> `f64`
* `Ascii`, `Text`, `Varchar` <----> `&str` and `String`
* `Counter` <----> `value::Counter`?
* `Blob` <----> `Vec<u8>`
* `Inet` <----> `std::net::IpAddr`
* `Uuid`, `Timeuuid` <----> `uuid::Uuid`
* `Date` <----> `chrono::NaiveDate`, `u32`
* `Time` <----> `chrono::Duration`
* `Timestamp` <----> `chrono::Duration`
* `Decimal` <----> `bigdecimal::Decimal`
* `Varint` <----> `num_bigint::BigInt`
* `List` <----> `Vec<T>`
* `Map` <----> `HashMap<K, V>`
* `Set` <----> `HashSet<V>`
* `Tuple` <----> Rust tuples
* `UDT` <----> Custom user defined types with macros