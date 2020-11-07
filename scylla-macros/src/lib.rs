extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(FromRow)]
pub fn from_row_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_from_row(&ast)
}

fn impl_from_row(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl From<scylla::frame::response::result::Row> for #name {
            fn from(row: scylla::frame::response::result::Row) -> Self {
                unimplemented!();
            }
        }
    };
    gen.into()
}
