use core::panic;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::RwLock,
};
use surrealdb::sql::Kind;

use blackbird_core::{
    read_migrations,
    schema::{get_schemas_from_migrations, is_nullable, TableSchema},
    Error,
};
use once_cell::unsync::Lazy;
use syn::{parse::Parse, parse_macro_input, Ident, LitStr, Token, Type};

const DEFAULT_MIGRATION_PATH: &str = "./src/migrations";
const SCHEMAS_BY_MIGRATION_PATH: Lazy<RwLock<HashMap<PathBuf, Vec<TableSchema>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

fn get_schema_from_local_cache<P>(migrations_directory: P) -> Result<Vec<TableSchema>, Error>
where
    P: AsRef<Path>,
{
    let path = migrations_directory.as_ref();
    if let Some(ts) = SCHEMAS_BY_MIGRATION_PATH
        .read()
        .ok()
        .and_then(|sbmp| sbmp.get(path).cloned())
    {
        Ok(ts)
    } else {
        let migs = read_migrations(&migrations_directory)?;

        // starting a runtime & block_on is horrible, can we just compress the schema affecting queries instead?
        let schemas = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(get_schemas_from_migrations(migs))?;

        SCHEMAS_BY_MIGRATION_PATH
            .write()
            .ok()
            .and_then(|mut sbmp| sbmp.insert(path.to_path_buf(), schemas.clone()));

        Ok(schemas)
    }
}

struct StructForTable {
    name: Ident,
    schema: TableSchema,
}

impl Parse for StructForTable {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;
        let rel_path: Option<LitStr> = input.parse()?;

        let migration_path = rel_path
            .map(|ls| ls.value())
            .unwrap_or_else(|| DEFAULT_MIGRATION_PATH.to_string());

        let schemas = get_schema_from_local_cache(&migration_path).map_err(|e| {
            syn::Error::new(
                name.span(),
                format!("Could not get migrations from {migration_path}: {e:?}"),
            )
        })?;

        let mut schemas_with_name = schemas
            .into_iter()
            .filter(|s| s.name.eq_ignore_ascii_case(&name.to_string()));

        let schema = schemas_with_name.next().ok_or_else(|| {
            syn::Error::new(
                name.span(),
                format!(
                    "Could not find table with name {name} in migrations from {migration_path}"
                ),
            )
        })?;

        if schemas_with_name.next().is_some() {
            return Err(syn::Error::new(
                name.span(),
                format!("More than one table with name {name} in migrations from {migration_path}"),
            ));
        }

        Ok(Self { name, schema })
    }
}

fn struct_field_for(field_name: &str, kind: Kind, nullable: bool) -> TokenStream {
    let typ: Type = match kind {
        Kind::Bool => Type::Verbatim(quote!(bool)),
        Kind::Datetime => todo!(),
        Kind::Decimal => todo!(),
        Kind::Duration => todo!(),
        Kind::Float => Type::Verbatim(quote!(f64)),
        Kind::Int => Type::Verbatim(quote!(i64)),
        Kind::Number => todo!(),
        Kind::String => Type::Verbatim(quote!(String)),

        // TODO: unhandled
        Kind::Object => todo!(),
        Kind::Record(_) => todo!(),
        Kind::Geometry(_) => todo!(),
        Kind::Any => todo!(),
        Kind::Array => todo!(),
    };

    let typ = if nullable {
        Type::Verbatim(quote!(Option<#typ>))
    } else {
        typ
    };
    let field_name = Ident::new(field_name, Span::call_site());

    quote! {
        pub #field_name: #typ,
    }
    .into()
}

#[proc_macro]
pub fn struct_for(input: TokenStream) -> TokenStream {
    let StructForTable { name, schema } = parse_macro_input!(input as StructForTable);

    let mut fields = Vec::with_capacity(schema.fields.len());

    for s in schema.fields {
        let nullable = is_nullable(&s);
        if let Some(kind) = s.kind {
            fields.push(struct_field_for(
                s.name.to_string().as_str(),
                kind,
                nullable,
            ));
        } else {
            // TODO: compile error instead
            panic!(
                "Field {} on table {} must have a defined type.",
                s.name, schema.name
            )
        }
    }

    let fields = fields
        .into_iter()
        .map(proc_macro2::TokenStream::from)
        .collect::<proc_macro2::TokenStream>();

    let expanded = quote! {
        struct #name {
            #fields
        }
    };

    TokenStream::from(expanded)
}
