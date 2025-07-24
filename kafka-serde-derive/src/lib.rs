use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input, punctuated::Punctuated};

#[proc_macro_derive(Encode)]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let expanded = match input.data {
        syn::Data::Struct(s) => derive_encode_for_struct(&name, s),
        _ => unimplemented!(),
    };

    TokenStream::from(expanded)
}

fn derive_encode_for_struct(
    struct_name: &syn::Ident,
    data: syn::DataStruct,
) -> proc_macro2::TokenStream {
    let fields = match data.fields {
        syn::Fields::Named(fields) => fields.named,
        syn::Fields::Unnamed(fields) => fields.unnamed,
        syn::Fields::Unit => Punctuated::new(),
    };

    let _inner_contents =
        fields
            .iter()
            .enumerate()
            .map(|(idx, field)| match field.ident.as_ref() {
                Some(name) => quote! {encode_vec.append(&mut self.#name.encode());},
                None => {
                    let _idx = syn::Index::from(idx);
                    quote! {encode_vec.append(&mut self.#_idx.encode());}
                }
            });

    quote! {
        impl Encode for #struct_name {
            fn encode(&self) -> Vec<u8> {
                let mut encode_vec = Vec::new();
                #(#_inner_contents)*
                encode_vec
            }
        }
    }
}
