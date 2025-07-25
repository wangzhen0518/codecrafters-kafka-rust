use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input, punctuated::Punctuated};

#[proc_macro_derive(Encode)]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let expanded = match input.data {
        syn::Data::Struct(data) => derive_encode_for_struct(&name, data),
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

#[proc_macro_derive(Decode)]
pub fn derive_decode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();
    let name = &input.ident;

    let expanded = match input.data {
        syn::Data::Struct(data) => {
            derive_decode_for_struct(name, impl_generics, ty_generics, where_clause, data)
        }
        _ => unimplemented!(),
    };

    TokenStream::from(expanded)
}

fn derive_decode_for_struct(
    struct_name: &syn::Ident,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: &Option<&syn::WhereClause>,
    data: syn::DataStruct,
) -> proc_macro2::TokenStream {
    match &data.fields {
        syn::Fields::Named(fields) => {
            let field_decodes = fields.named.iter().map(|field| {
                let field_name = field.ident.as_ref().unwrap();
                let field_type = &field.ty;
                quote! { #field_name: <#field_type as Decode>::decode(buffer)? }
            });

            quote! {
                impl #impl_generics Decode for #struct_name #ty_generics #where_clause {
                    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::decode::DecodeError> {
                        Ok(Self {
                            #(#field_decodes,)*
                        })
                    }
                }
            }
        }
        syn::Fields::Unnamed(fields) => {
            let field_decodes = fields.unnamed.iter().map(|field| {
                let field_type = &field.ty;
                quote! {  <#field_type as Decode>::decode(buffer)? }
            });

            quote! {
                impl #impl_generics Decode for #struct_name #ty_generics #where_clause {
                    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::decode::DecodeError> {
                        Self (
                            #(#field_decodes,)*
                        )
                    }
                }
            }
        }
        syn::Fields::Unit => unimplemented!(),
    }
}
