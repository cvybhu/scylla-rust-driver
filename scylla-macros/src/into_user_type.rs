use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields};

/// #[derive(IntoUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
pub fn into_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens_input as DeriveInput);

    let struct_name = &input.ident;

    let struct_fields = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(named_fields) => named_fields,
                _ => panic!("derive(IntoUserType) works only for structs with named fields. Tuples don't need derive."),
            }
        },
        _ => panic!("derive(IntoUserType) works only on structs!")
    };

    let serialize_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;

        quote_spanned! {field.span() =>
            total_size += <_ as SerializeAsValue>::serialize(&self.#field_name, buf) ?;
        }
    });

    let generated = quote! {
        impl scylla::frame::value::SerializeAsValue for #struct_name {
            fn serialize(&self, buf: &mut scylla::macros::BytesMut) -> std::result::Result<usize, scylla::frame::value::ValueTooBig> {
                use scylla::frame::value::{SerializeAsValue, ValueTooBig};
                use scylla::macros::{BytesMut, BufMut};
                use ::std::convert::TryInto;

                let mut total_size: usize = 0;

                // Reserve space to put serialized size in
                let total_size_index: usize = buf.len();
                buf.put_i32(0);

                // Serialize fields
                #(#serialize_code)*

                // Put serialized size in it's place
                let total_size_i32: i32 = total_size.try_into().map_err(|_| ValueTooBig) ?;
                buf[total_size_index..(total_size_index+4)].copy_from_slice(&total_size_i32.to_be_bytes()[..]);

                // Return total written size
                Ok(total_size + 4)
            }
        }
    };

    TokenStream::from(generated)
}
