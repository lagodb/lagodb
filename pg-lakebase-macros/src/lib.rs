extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, TokenStreamExt, format_ident, quote};
use syn::{
    ItemStruct, Lit, LitStr, MetaNameValue, Token, parse_macro_input,
    punctuated::Punctuated,
};

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
///     type DmlSession = MyModify;
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
/// struct MyModify;
/// impl AmDmlSession for MyModify { /* ... */ }
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
