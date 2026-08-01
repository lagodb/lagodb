extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, TokenStreamExt, format_ident, quote};
use syn::{
    ItemStruct, Lit, LitStr, Meta, MetaNameValue, Token, parse_macro_input,
    punctuated::Punctuated,
};

/// Generate PostgreSQL handler, validator, and metadata functions for an FDW.
///
/// The provider's `ForeignDataWrapper::register` implementation calls
/// `register_scan` and/or `register_modify`. Keeping those calls in the trait
/// implementation avoids declaring the same capability a second time in the
/// attribute. A procedural macro cannot inspect trait implementations
/// elsewhere in the crate on stable Rust.
///
/// Metadata arguments are optional. The macro accepts only `version`,
/// `author`, and `website`; scan and modify capabilities are registered by the
/// provider's `register` implementation.
/// Use `#[pg_fdw]` when the provider has no metadata values to publish.
///
/// ```rust,ignore
/// use core::ffi::CStr;
/// use pg_lakebase_core::fdw::prelude::*;
/// use pg_lakebase_core::pg_fdw;
///
/// #[pg_fdw(
///     version = "0.1.0",
///     author = "Author",
///     website = "https://example.com"
/// )]
/// pub struct MyFdw;
///
/// impl ForeignDataWrapper for MyFdw {
///     const NAME: &'static CStr = c"my_fdw";
///
///     fn register(routine: &mut FdwRoutine) {
///         register_scan::<Self>(routine);
///         // register_modify::<Self>(routine); // for a combined provider
///     }
///
///     fn validate(
///         options: &[Option<String>],
///         catalog: Option<pgrx::pg_sys::Oid>,
///     ) -> Result<(), ForeignValidationError> {
///         let _ = (options, catalog);
///         Ok(())
///     }
/// }
/// ```
///
/// The generated SQL functions can be used as follows:
///
/// ```sql
/// CREATE FOREIGN DATA WRAPPER my_fdw
///   HANDLER my_fdw_fdw_handler
///   VALIDATOR my_fdw_fdw_validator;
///
/// SELECT * FROM my_fdw_fdw_meta();
/// ```
#[proc_macro_attribute]
pub fn pg_fdw(attr: TokenStream, item: TokenStream) -> TokenStream {
    let meta_attrs: Punctuated<Meta, Token![,]> =
        parse_macro_input!(attr with Punctuated::parse_terminated);
    let mut version = None;
    let mut author = None;
    let mut website = None;

    for meta in meta_attrs {
        let Meta::NameValue(meta) = meta else {
            return syn::Error::new_spanned(
                meta,
                "`pg_fdw` accepts only version, author, and website string metadata",
            )
            .to_compile_error()
            .into();
        };

        if meta.path.segments.len() != 1 {
            return syn::Error::new_spanned(
                meta.path,
                "FDW metadata keys must be simple identifiers",
            )
            .to_compile_error()
            .into();
        }

        let name = meta.path.segments[0].ident.clone();
        let value = match meta.lit {
            Lit::Str(value) => value,
            literal => {
                return syn::Error::new_spanned(
                    literal,
                    "FDW metadata values must be string literals",
                )
                .to_compile_error()
                .into();
            }
        };

        let target = match name.to_string().as_str() {
            "version" => &mut version,
            "author" => &mut author,
            "website" => &mut website,
            _ => {
                return syn::Error::new_spanned(
                    &name,
                    "unknown FDW metadata key; expected version, author, or website",
                )
                .to_compile_error()
                .into();
            }
        };

        if target.is_some() {
            return syn::Error::new_spanned(&name, "duplicate FDW metadata key")
                .to_compile_error()
                .into();
        }
        *target = Some(value);
    }

    let version = metadata_value(version);
    let author = metadata_value(author);
    let website = metadata_value(website);

    let item: ItemStruct = parse_macro_input!(item as ItemStruct);
    let item_tokens = item.to_token_stream();
    let ident = item.ident;
    let ident_str = ident.to_string();
    let ident_snake = to_snake_case(ident_str.as_str());
    let module_ident = format_ident!("__{}_fdw", ident_snake);
    let fn_ident = format_ident!("{}_fdw_handler", ident_snake);
    let fn_validator_ident = format_ident!("{}_fdw_validator", ident_snake);
    let fn_meta_ident = format_ident!("{}_fdw_meta", ident_snake);

    let sql = format!(
        "CREATE OR REPLACE FUNCTION {0}() RETURNS fdw_handler LANGUAGE c STRICT AS 'MODULE_PATHNAME', '{0}_wrapper';",
        fn_ident
    );
    let sql_lit = LitStr::new(&sql, Span::call_site());
    quote! {
        #item_tokens

        impl #ident {
            pub fn fdw_routine() -> pg_lakebase_core::fdw::FdwRoutine {
                let mut routine = pg_lakebase_core::fdw::__private::new_routine();
                <#ident as pg_lakebase_core::fdw::ForeignDataWrapper>::register(&mut routine);
                routine
            }
        }

        mod #module_ident {
            use super::#ident;
            use pg_lakebase_core::diag::ReportableError;
            use pg_lakebase_core::fdw::ForeignDataWrapper;
            use pgrx::pg_sys::panic::ErrorReportable;
            use pgrx::prelude::*;

            #[pg_extern(create_or_replace, sql = #sql_lit)]
            fn #fn_ident() -> pg_lakebase_core::fdw::FdwRoutine {
                #ident::fdw_routine()
            }

            #[pg_extern(create_or_replace)]
            fn #fn_validator_ident(
                options: Vec<Option<String>>,
                catalog: Option<pg_sys::Oid>,
            ) {
                <#ident as pg_lakebase_core::fdw::ForeignDataWrapper>::validate(
                    &options,
                    catalog,
                )
                .report_unwrap();
            }

            #[pg_extern(create_or_replace)]
            fn #fn_meta_ident() -> TableIterator<'static, (
                name!(name, Option<String>),
                name!(version, Option<String>),
                name!(author, Option<String>),
                name!(website, Option<String>)
            )> {
                TableIterator::once((
                    Some(
                        <#ident as ForeignDataWrapper>::NAME
                            .to_str()
                            .unwrap_or_report()
                            .to_owned(),
                    ),
                    #version,
                    #author,
                    #website,
                ))
            }
        }
    }
    .into()
}

fn metadata_value(value: Option<LitStr>) -> TokenStream2 {
    match value {
        Some(value) => quote! { Some(#value.to_owned()) },
        None => quote! { None },
    }
}

/// Create necessary handler and meta functions for a PostgreSQL Table Access Method
///
/// This macro will create two functions which can be used in Postgres.
///
/// 1. `<snake_case_am_name>_am_handler()` - table access method handler function
/// 2. `<snake_case_am_name>_am_meta()` - function to return a table contains am metadata
///
/// # Example
///
/// ```rust,ignore
/// use pg_lakebase_core::prelude::*;
///
/// #[pg_table_am(
///     version = "0.1.0",
///     author = "Your Name",
///     website = "https://github.com/your/repo"
/// )]
/// pub struct MyTableAm;
///
/// impl TableAccessMethod for MyTableAm {
///     type ScanSession = MyScan;
///     type IndexFetchSession = MyIndexFetch;
///     type ModifyQueryState = MyModifyQueryState;
///     type ModifyState = MyModify;
///     type CopySession = MyCopy;
/// }
///
/// struct MyScan;
/// impl AmScan for MyTableAm { /* ... */ }
/// impl AmScanSession for MyScan { /* ... */ }
///
/// impl AmRelation for MyTableAm { /* ... */ }
///
/// struct MyIndexFetch;
/// impl AmIndexFetchSession for MyIndexFetch { /* ... */ }
/// impl AmIndexCallbacks for MyTableAm { /* ... */ }
///
/// impl AmDdl for MyTableAm { /* ... */ }
///
/// struct MyModifyQueryState;
/// impl AmModifyQueryState for MyModifyQueryState { /* ... */ }
///
/// struct MyModify;
/// impl AmModifyState for MyModify { /* ... */ }
///
/// struct MyCopy;
/// impl AmCopySession for MyCopy { /* ... */ }
/// ```
///
/// then you can use those functions in Postgres,
///
/// ```sql
/// create extension pg_lakebase_core
///
/// create function my_table_am_handler() returns table_am_handler
/// create access method my_table_am type table handler my_table_am_handler;
///
/// select * from my_table_am_meta();
/// ```
#[proc_macro_attribute]
pub fn pg_table_am(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut metas = TokenStream2::new();
    let meta_attrs: Punctuated<MetaNameValue, Token![,]> =
        parse_macro_input!(attr with Punctuated::parse_terminated);
    for attr in meta_attrs {
        let name = format!("{}", attr.path.segments.first().unwrap().ident);
        if let Lit::Str(val) = attr.lit {
            let value = val.value();
            if name == "version" || name == "author" || name == "website" {
                metas.append_all(quote! {
                    meta.insert(#name.to_owned(), #value.to_owned());
                });
            }
        }
    }

    let item: ItemStruct = parse_macro_input!(item as ItemStruct);
    let item_tokens = item.to_token_stream();
    let ident = item.ident;
    let ident_str = ident.to_string();
    let ident_snake = to_snake_case(ident_str.as_str());

    let module_ident = format_ident!("__{}_pgrx", ident_snake);
    let fn_ident = format_ident!("{}_handler", ident_snake);

    let sql_def = format!(
        "CREATE OR REPLACE FUNCTION {0}(internal) RETURNS table_am_handler LANGUAGE c STRICT AS 'MODULE_PATHNAME', '{0}_wrapper';",
        fn_ident
    );
    let sql_def_lit = LitStr::new(&sql_def, Span::call_site());

    let fn_meta_ident = format_ident!("{}_am_meta", ident_snake);
    let fn_get_meta_ident = format_ident!("{}_get_meta", ident_snake);

    let quoted = quote! {
        #item_tokens

        impl #ident {
            pub fn cached_am_routine() -> pg_lakebase_core::TableAmRoutine {
                #module_ident::cached_am_routine()
            }
        }

        mod #module_ident {
            use super::#ident;
            use std::collections::HashMap;
            use std::sync::OnceLock;
            use pgrx::prelude::*;
            use pg_lakebase_core::prelude::*;

            struct AmRoutinePtr(*mut pgrx::pg_sys::TableAmRoutine);
            unsafe impl Send for AmRoutinePtr {}
            unsafe impl Sync for AmRoutinePtr {}

            static AM_ROUTINE: OnceLock<AmRoutinePtr> = OnceLock::new();

            pub(super) fn cached_am_routine() -> pg_lakebase_core::TableAmRoutine {
                let ptr = AM_ROUTINE
                    .get_or_init(|| {
                        let routine = #ident::am_routine();
                        AmRoutinePtr(routine.into_pg())
                    })
                    .0;

                unsafe { pg_lakebase_core::TableAmRoutine::from_pg(ptr) }
            }

            #[pg_extern(create_or_replace, sql = #sql_def_lit)]
            fn #fn_ident() -> pg_lakebase_core::TableAmRoutine {
                #ident::cached_am_routine()
            }

            pub(super) fn #fn_get_meta_ident() -> HashMap<String, String> {
                let mut meta: HashMap<String, String> = HashMap::new();
                #metas
                meta
            }

            #[pg_extern(create_or_replace)]
            fn #fn_meta_ident() -> TableIterator<'static, (
                name!(name, Option<String>),
                name!(version, Option<String>),
                name!(author, Option<String>),
                name!(website, Option<String>)
            )> {
                let meta = #fn_get_meta_ident();

                TableIterator::new(vec![(
                    Some(#ident_str.to_owned()),
                    meta.get("version").map(|s| s.to_owned()),
                    meta.get("author").map(|s| s.to_owned()),
                    meta.get("website").map(|s| s.to_owned()),
                )].into_iter())
            }
        }

    };

    quoted.into()
}

fn to_snake_case(s: &str) -> String {
    let mut acc = String::new();
    let mut prev = '_';
    for ch in s.chars() {
        if ch.is_uppercase() && prev != '_' {
            acc.push('_');
        }
        acc.push(ch);
        prev = ch;
    }
    acc.to_lowercase()
}
