//! Procedural derives for declarative command-token argument shapes.

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;

use syn::parse_macro_input;
use syn::{Attribute, Data, DeriveInput, Fields, FieldsNamed, Ident, Type};

#[proc_macro_derive(CommandSpec, attributes(command_spec, positional))]
pub fn derive_command_spec(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_command_spec(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(OptionGroupSpec, attributes(option_spec))]
pub fn derive_option_group_spec(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_option_group_spec(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn find_command_spec(attrs: &[Attribute]) -> syn::Result<(String, bool, Option<u32>)> {
    for attr in attrs {
        if !attr.path().is_ident("command_spec") {
            continue;
        }
        let mut name_opt: Option<String> = None;
        let mut ignore_remaining = false;
        let mut exact_tail: Option<u32> = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let v: syn::LitStr = meta.value()?.parse()?;
                name_opt = Some(v.value());
                Ok(())
            } else if meta.path.is_ident("ignore_remaining") {
                ignore_remaining = true;
                Ok(())
            } else if meta.path.is_ident("exact_tail_tokens") {
                let v: syn::LitInt = meta.value()?.parse()?;
                exact_tail = Some(v.base10_parse()?);
                Ok(())
            } else if meta.path.is_ident("option_group") {
                Ok(())
            } else {
                Err(meta.error("unknown command_spec item"))
            }
        })?;
        if let Some(name) = name_opt {
            return Ok((name, ignore_remaining, exact_tail));
        }
    }
    Err(syn::Error::new(
        Span::call_site(),
        "missing #[command_spec(name = \"...\")] on struct",
    ))
}

#[derive(Clone, Copy)]
enum Cardinality {
    ExactlyOne,
    OneOrMany,
}

#[derive(Clone, Copy)]
struct PositionalCfg {
    cardinality: Cardinality,
    utf8_echo: bool,
}

fn parse_positional(attr: &Attribute) -> syn::Result<PositionalCfg> {
    let mut cardinality = Cardinality::ExactlyOne;
    let mut utf8_echo = false;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("cardinality") {
            let v: Ident = meta.value()?.parse()?;
            cardinality = match v.to_string().as_str() {
                "exactly_one" => Cardinality::ExactlyOne,
                "one_or_many" => Cardinality::OneOrMany,
                _ => return Err(meta.error("expected exactly_one | one_or_many")),
            };
            Ok(())
        } else if meta.path.is_ident("utf8_echo") {
            utf8_echo = true;
            Ok(())
        } else {
            Err(meta.error("unknown positional item"))
        }
    })?;
    Ok(PositionalCfg {
        cardinality,
        utf8_echo,
    })
}

#[derive(Clone, Copy)]
enum FieldRole {
    Positional(PositionalCfg),
    OptionGroup,
}

fn field_has_option_group(a: &Attribute) -> syn::Result<bool> {
    if !a.path().is_ident("command_spec") {
        return Ok(false);
    }
    Ok(matches!(
        &a.meta,
        syn::Meta::List(list) if list.tokens.to_string().trim() == "option_group"
    ))
}

fn parse_field_role(attrs: &[Attribute]) -> syn::Result<Option<FieldRole>> {
    let mut positional: Option<PositionalCfg> = None;
    let mut option_group = false;
    for a in attrs {
        if a.path().is_ident("positional") {
            if positional.replace(parse_positional(a)?).is_some() {
                return Err(syn::Error::new_spanned(a, "duplicate positional"));
            }
        } else if field_has_option_group(a)? {
            option_group = true;
        }
    }
    match (positional, option_group) {
        (Some(_), true) => Err(syn::Error::new(
            Span::call_site(),
            "field cannot be both positional and option_group",
        )),
        (Some(p), false) => Ok(Some(FieldRole::Positional(p))),
        (None, true) => Ok(Some(FieldRole::OptionGroup)),
        (None, false) => Ok(None),
    }
}

enum StructFields<'a> {
    Named(&'a FieldsNamed),
    Unit,
}

fn struct_fields(ds: &Data) -> syn::Result<StructFields<'_>> {
    match ds {
        Data::Struct(st) => match &st.fields {
            Fields::Named(n) => Ok(StructFields::Named(n)),
            Fields::Unit => Ok(StructFields::Unit),
            Fields::Unnamed(_) => Err(syn::Error::new_spanned(
                &st.struct_token,
                "CommandSpec expects named fields or unit struct `StructName;`",
            )),
        },
        _ => Err(syn::Error::new(
            Span::call_site(),
            "CommandSpec expects a struct",
        )),
    }
}

fn is_type_isize(ty: &Type) -> bool {
    matches!(ty, Type::Path(p)
        if p.qself.is_none()
            && p.path.segments.len() == 1
            && p.path.segments[0].ident == "isize")
}

fn is_type_bytes(ty: &Type) -> bool {
    matches!(ty, Type::Path(p)
        if p.qself.is_none()
            && p.path.segments.last().is_some_and(|s| s.ident == "Bytes"))
}

fn is_type_vec_bytes(ty: &Type) -> bool {
    if let Type::Path(p) = ty {
        if p.qself.is_some() {
            return false;
        }
        let seg = match p.path.segments.last() {
            Some(s) => s,
            None => return false,
        };
        if seg.ident != "Vec" {
            return false;
        }
        if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
            if let Some(syn::GenericArgument::Type(Type::Path(inner))) = ab.args.first() {
                return inner.path.segments.last().is_some_and(|s| s.ident == "Bytes");
            }
        }
    }
    false
}

fn expand_command_spec(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let (cmd_name_str, ignore_remaining, exact_tail_tokens) = find_command_spec(&input.attrs)?;

    let struct_ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let cmd_name_lit = syn::LitStr::new(&cmd_name_str, Span::call_site());

    match struct_fields(&input.data)? {
        StructFields::Unit if !ignore_remaining => {
            return Err(syn::Error::new_spanned(&struct_ident, "unit structs require #[command_spec(ignore_remaining)]"));
        }
        _ => (),
    };

    let data = struct_fields(&input.data)?;

    let mut stmts = proc_macro2::TokenStream::new();

    if let Some(n) = exact_tail_tokens {
        stmts.extend(quote! {
            if tokens.len() != (#n as usize) {
                return Err(crate::command::CommandError::InvalidArgument(
                    crate::command::parse_errors::WRONG_ARGUMENT_COUNT,
                ));
            }
        });
    }

    stmts.extend(quote! { let mut idx: usize = 0usize; });

    let named_iter: Box<dyn Iterator<Item = &syn::Field>> = match &data {
        StructFields::Named(n) => Box::new(n.named.iter()),
        StructFields::Unit => Box::new(std::iter::empty()),
    };

    let mut struct_fields_construct: Vec<proc_macro2::TokenStream> = Vec::new();

    for field in named_iter {
        let fname = field.ident.as_ref().unwrap();
        let fty = &field.ty;
        let role = parse_field_role(&field.attrs)?.ok_or_else(|| {
            syn::Error::new_spanned(field, "use #[positional(...)] or #[command_spec(option_group)]")
        })?;
        match role {
            FieldRole::OptionGroup => {
                stmts.extend(quote! {
                    let #fname =
                        <#fty as crate::command::spec_parse::OptionGroupParser>::parse_option_group(
                            tokens, &mut idx,
                        )?;
                });
                struct_fields_construct.push(quote!(#fname));
            }
            FieldRole::Positional(cfg) => {
                stmts.extend(quote_positional_stmts(fname, fty, cfg)?);
                struct_fields_construct.push(quote!(#fname));
            }
        }
    }

    let finish_tokens = if ignore_remaining {
        quote! {}
    } else if exact_tail_tokens.is_some() {
        quote! {
            debug_assert!(
                idx == tokens.len(),
                "exact-tail commands should consume all tokens"
            );
        }
    } else {
        quote! {
            if idx != tokens.len() {
                return Err(crate::command::CommandError::InvalidArgument(
                    crate::command::parse_errors::EXTRA_TRAILING,
                ));
            }
        }
    };

    let build_self = match &data {
        StructFields::Unit => quote!(Self),
        StructFields::Named(_) => {
            let f = &struct_fields_construct;
            quote!(Self { #( #f ),* })
        }
    };

    Ok(quote! {
        impl #impl_generics crate::command::spec_parse::CommandSyntax for #struct_ident #ty_generics
        #where_clause
        {
            const COMMAND_NAME: &'static str = #cmd_name_lit;

            fn try_from_tail(
                parsed_tail: crate::command::ParsedTail<'_>,
            ) -> ::core::result::Result<Self, crate::command::CommandError> {
                let tokens = parsed_tail.0;
                #stmts
                #finish_tokens
                ::core::result::Result::Ok(#build_self)
            }
        }
    })
}

fn quote_positional_stmts(
    fname: &Ident,
    ty: &Type,
    cfg: PositionalCfg,
) -> syn::Result<proc_macro2::TokenStream> {
    match cfg.cardinality {
        Cardinality::ExactlyOne => {
            if is_type_isize(ty) {
                return Ok(quote! {
                    let #fname =
                        crate::command::spec_parse::parse_isize_token(tokens, &mut idx)?;
                });
            }
            if !is_type_bytes(ty) {
                return Err(syn::Error::new_spanned(
                    ty,
                    "positional exactly_one expects Bytes or isize",
                ));
            }
            let utf_gate = if cfg.utf8_echo {
                quote! {
                    ::core::str::from_utf8(#fname.as_ref()).map_err(|_| {
                        crate::command::CommandError::InvalidArgument(
                            crate::command::parse_errors::INVALID_UTF8,
                        )
                    })?;
                }
            } else {
                quote! {}
            };
            Ok(quote! {
                if idx >= tokens.len() {
                    return Err(crate::command::CommandError::InvalidArgument(
                        crate::command::parse_errors::MISSING_ARGUMENT,
                    ));
                }
                let #fname: bytes::Bytes = tokens[idx].clone();
                idx += 1usize;
                #utf_gate
            })
        }
        Cardinality::OneOrMany => {
            if !(is_type_vec_bytes(ty)) {
                return Err(syn::Error::new_spanned(
                    ty,
                    "one_or_many expects Vec<Bytes>",
                ));
            }
            Ok(quote! {
                if idx >= tokens.len() {
                    return Err(crate::command::CommandError::InvalidArgument(
                        crate::command::parse_errors::MISSING_ARGUMENT,
                    ));
                }
                let #fname: ::std::vec::Vec<bytes::Bytes> = tokens[idx..].to_vec();
                if #fname.is_empty() {
                    return Err(crate::command::CommandError::InvalidArgument(
                        crate::command::parse_errors::WRONG_ARGUMENT_COUNT,
                    ));
                }
                idx = tokens.len();
            })
        }
    }
}

fn quote_byte_slice_lit(s: &str) -> proc_macro2::TokenStream {
    let bytes: Vec<u8> = s.bytes().collect();
    quote! { &[ #( #bytes ),* ] }
}

fn expand_option_group_spec(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let enum_ident = &input.ident;
    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let Data::Enum(en) = &input.data else {
        return Err(syn::Error::new_spanned(enum_ident, "OptionGroupSpec only supports enums"));
    };

    let mut absent_ident: Option<Ident> = None;

    #[derive(Clone)]
    struct KwVar {
        kw: String,
        variant: Ident,
    }
    let mut keyword_checks: Vec<KwVar> = Vec::new();

    for v in &en.variants {
        let mut is_absent = false;
        let mut kw_str: Option<String> = None;

        for a in &v.attrs {
            if !a.path().is_ident("option_spec") {
                continue;
            }
            a.parse_nested_meta(|meta| {
                if meta.path.is_ident("absent") {
                    is_absent = true;
                    Ok(())
                } else if meta.path.is_ident("keyword") {
                    let lt: syn::LitStr = meta.value()?.parse()?;
                    kw_str = Some(lt.value());
                    Ok(())
                } else {
                    Err(meta.error("unknown option_spec item"))
                }
            })?;
        }

        match (is_absent, kw_str) {
            (true, Some(_)) => {
                return Err(syn::Error::new_spanned(v, "variant cannot be both absent and keyword"));
            }
            (true, None) => {
                if !matches!(&v.fields, Fields::Unit) {
                    return Err(syn::Error::new_spanned(
                        v,
                        "`#[option_spec(absent)]` variant must be a unit variant",
                    ));
                }
                if absent_ident.replace(v.ident.clone()).is_some() {
                    return Err(syn::Error::new_spanned(v, "only one absent variant allowed"));
                }
            }
            (false, Some(kw)) => {
                let flds = match &v.fields {
                    Fields::Unnamed(u) if u.unnamed.len() == 1 => &u.unnamed,
                    _ => {
                        return Err(syn::Error::new_spanned(
                            v,
                            "keyword option variant must have a single unnamed field (payload)",
                        ));
                    }
                };
                let inner = &flds.first().unwrap().ty;
                if !is_type_bytes(inner) {
                    return Err(syn::Error::new_spanned(
                        flds.first().unwrap(),
                        "keyword option payload must be Bytes",
                    ));
                }
                keyword_checks.push(KwVar {
                    kw,
                    variant: v.ident.clone(),
                });
            }
            (false, None) => {
                return Err(syn::Error::new_spanned(
                    v,
                    "variant needs #[option_spec(absent)] or #[option_spec(keyword = \"\")]",
                ));
            }
        }
    }

    let absent =
        absent_ident.ok_or_else(|| syn::Error::new_spanned(enum_ident, "missing #[option_spec(absent)] variant"))?;

    let mut branches = Vec::new();
    for kv in &keyword_checks {
        let pat = quote_byte_slice_lit(&kv.kw);
        let var = &kv.variant;
        branches.push(quote! {
            {
                let expected: &[u8] = #pat;
                if h == expected {
                    *idx += 1usize;
                    if *idx >= tokens.len() {
                        return Err(crate::command::CommandError::InvalidArgument(
                            crate::command::parse_errors::MISSING_OPTION_ARGUMENT,
                        ));
                    }
                    let payload: bytes::Bytes = tokens[*idx].clone();
                    *idx += 1usize;
                    return ::core::result::Result::Ok(#enum_ident::#var(payload));
                }
            }
        });
    }

    Ok(quote! {
        impl #impl_generics crate::command::spec_parse::OptionGroupParser for #enum_ident #ty_generics
        #where_clause
        {
            fn parse_option_group(
                tokens: &[bytes::Bytes],
                idx: &mut usize,
            ) -> ::core::result::Result<Self, crate::command::CommandError> {
                if *idx >= tokens.len() {
                    return ::core::result::Result::Ok(#enum_ident::#absent);
                }

                let h = tokens[*idx].as_ref();
                #( #branches )*
                Err(crate::command::CommandError::InvalidArgument(
                    crate::command::parse_errors::INVALID_OPTION_TOKEN,
                ))
            }
        }
    })
}
