use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Lit, LitStr, Meta, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

/// Parsed metadata shared by the FDW and table-access-method macros.
pub(crate) struct ProviderMetadata {
    version: Option<LitStr>,
    author: Option<LitStr>,
    website: Option<LitStr>,
}

impl ProviderMetadata {
    /// Converts the parsed values into generated `Option<String>` expressions.
    pub(crate) fn into_tokens(self) -> (TokenStream, TokenStream, TokenStream) {
        (
            Self::value_tokens(self.version),
            Self::value_tokens(self.author),
            Self::value_tokens(self.website),
        )
    }

    fn value_tokens(value: Option<LitStr>) -> TokenStream {
        match value {
            Some(value) => {
                quote! { ::core::option::Option::Some(::std::string::String::from(#value)) }
            }
            None => quote! { ::core::option::Option::None },
        }
    }
}

impl Parse for ProviderMetadata {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let meta_attrs = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut version = None;
        let mut author = None;
        let mut website = None;

        for meta in meta_attrs {
            let Meta::NameValue(meta) = meta else {
                return Err(syn::Error::new_spanned(
                    meta,
                    "provider metadata accepts only version, author, and website string values",
                ));
            };

            if meta.path.segments.len() != 1 {
                return Err(syn::Error::new_spanned(
                    meta.path,
                    "provider metadata keys must be simple identifiers",
                ));
            }

            let name = meta.path.segments[0].ident.clone();
            let value = match meta.lit {
                Lit::Str(value) => value,
                literal => {
                    return Err(syn::Error::new_spanned(
                        literal,
                        "provider metadata values must be string literals",
                    ));
                }
            };

            let target = match name.to_string().as_str() {
                "version" => &mut version,
                "author" => &mut author,
                "website" => &mut website,
                _ => {
                    return Err(syn::Error::new_spanned(
                        &name,
                        "unknown provider metadata key; expected version, author, or website",
                    ));
                }
            };

            if target.is_some() {
                return Err(syn::Error::new_spanned(
                    &name,
                    "duplicate provider metadata key",
                ));
            }
            *target = Some(value);
        }

        Ok(Self {
            version,
            author,
            website,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderMetadata;

    #[test]
    fn accepts_empty_metadata() {
        syn::parse_str::<ProviderMetadata>("")
            .expect("empty metadata must be accepted");
    }

    #[test]
    fn accepts_supported_string_metadata() {
        syn::parse_str::<ProviderMetadata>(
            r#"version = "0.1.0", author = "LagoDB", website = "https://lagodb.dev""#,
        )
        .expect("supported string metadata must be accepted");
    }

    #[test]
    fn rejects_unknown_metadata() {
        let error = syn::parse_str::<ProviderMetadata>(r#"unknown = "value""#)
            .err()
            .expect("unknown metadata must be rejected");
        assert_eq!(
            error.to_string(),
            "unknown provider metadata key; expected version, author, or website"
        );
    }

    #[test]
    fn rejects_non_string_metadata() {
        let error = syn::parse_str::<ProviderMetadata>("version = 1")
            .err()
            .expect("non-string metadata must be rejected");
        assert_eq!(
            error.to_string(),
            "provider metadata values must be string literals"
        );
    }

    #[test]
    fn rejects_duplicate_metadata() {
        let error = syn::parse_str::<ProviderMetadata>(
            r#"version = "0.1.0", version = "0.2.0""#,
        )
        .err()
        .expect("duplicate metadata must be rejected");
        assert_eq!(error.to_string(), "duplicate provider metadata key");
    }

    #[test]
    fn rejects_qualified_metadata_keys() {
        let error =
            syn::parse_str::<ProviderMetadata>(r#"version::unexpected = "0.1.0""#)
                .err()
                .expect("qualified metadata keys must be rejected");
        assert_eq!(
            error.to_string(),
            "provider metadata keys must be simple identifiers"
        );
    }

    #[test]
    fn rejects_non_name_value_metadata() {
        let error = syn::parse_str::<ProviderMetadata>("version")
            .err()
            .expect("non-name-value metadata must be rejected");
        assert_eq!(
            error.to_string(),
            "provider metadata accepts only version, author, and website string values"
        );
    }
}
