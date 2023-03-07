use std::{
    collections::BTreeMap,
    fmt::Display,
    fs::{self},
    path::{Path, PathBuf},
};

use surrealdb::{
    sql::{
        parse,
        statements::{
            DefineDatabaseStatement, DefineFieldStatement, DefineNamespaceStatement,
            DefineStatement, DefineTableStatement, InfoStatement,
        },
        Object, Query, Statement, Statements, Value,
    },
    Datastore, Session,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    DbError(#[from] surrealdb::Error),

    #[error("Expected {0} rows, got {1}")]
    UnexpectedResultCount(usize, usize),

    #[error("Expected type {0}, but got {1}")]
    UnexpectedType(String, String),

    #[error("Object is missing expected key: {0}")]
    MissingExpectedKey(String),

    #[error("IO error: {context}")]
    IOError {
        context: String,
        source: std::io::Error,
    },
}

// Cribbed from anyhow, and slightly modified to line up with our error above, and be less generic
trait Context<T, E> {
    fn context(self, context: String) -> Result<T, Error>;

    fn with_context<F>(self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> String;
}

impl<T> Context<T, std::io::Error> for std::io::Result<T> {
    fn context(self, context: String) -> Result<T, Error> {
        return self.map_err(|source| Error::IOError { context, source });
    }

    fn with_context<F>(self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> String,
    {
        return self.map_err(|source| Error::IOError {
            context: f(),
            source,
        });
    }
}

pub async fn run_single_statement(
    ds: &Datastore,
    sess: &Session,
    query: Statement,
    vars: Option<BTreeMap<String, Value>>,
) -> Result<Value, Error> {
    let results = run_statements(ds, sess, vec![query], vars).await;

    if results.len() != 1 {
        return Err(Error::UnexpectedResultCount(1, results.len()));
    }

    let mut results_iter = results.into_iter();

    if let Some(result) = results_iter.next() {
        return Ok(result?);
    } else {
        return Err(Error::UnexpectedResultCount(1, 0));
    }
}

pub async fn run_statements(
    ds: &Datastore,
    sess: &Session,
    queries: Vec<Statement>,
    vars: Option<BTreeMap<String, Value>>,
) -> Vec<Result<Value, surrealdb::Error>> {
    let resp = match ds
        .process(Query(Statements(queries)), sess, vars, true)
        .await
    {
        Ok(resp) => resp,
        Err(e) => return vec![Err(e)],
    };

    resp.into_iter().map(|r| r.result).collect()
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub definition: DefineTableStatement,
    pub fields: Vec<DefineFieldStatement>,
}

impl Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{};\n", self.definition,))?;
        for field in &self.fields {
            f.write_fmt(format_args!("{field};\n"))?;
        }

        Ok(())
    }
}

impl TableSchema {
    fn from_tables_info_object(info_obj: Object) -> Result<Vec<TableSchema>, Error> {
        let tb_val = info_obj
            .0
            .get("tb")
            .ok_or_else(|| Error::MissingExpectedKey("tb".to_string()))?;

        match tb_val {
            Value::Object(o) => return o.0.iter().map(Self::from_key_and_val).collect(),
            t => {
                return Err(Error::UnexpectedType(
                    "Object".to_string(),
                    format!("{:?}", t),
                ))
            }
        }
    }

    fn from_key_and_val((name, def_str_val): (&String, &Value)) -> Result<TableSchema, Error> {
        let define_statement = match parse_to_define_statement(def_str_val)? {
            DefineStatement::Table(s) => s,
            t => {
                return Err(Error::UnexpectedType(
                    "DefineStatement::Table".to_string(),
                    format!("{:?}", t),
                ))
            }
        };

        Ok(TableSchema {
            name: name.clone(),
            definition: define_statement,
            fields: Vec::default(),
        })
    }
}

fn parse_to_define_statement(val: &Value) -> Result<DefineStatement, Error> {
    let query = match val {
        Value::Strand(s) => parse(s.0.as_str())?,
        t => {
            return Err(Error::UnexpectedType(
                "Value::Strand".to_string(),
                format!("{:?}", t),
            ))
        }
    };

    let statement = query
        .0
         .0
        .into_iter()
        .next()
        .ok_or_else(|| Error::UnexpectedResultCount(1, 0))?;

    match statement {
        Statement::Define(s) => Ok(s),
        t => Err(Error::UnexpectedType(
            "Statement::Define".to_string(),
            format!("{:?}", t),
        )),
    }
}

pub async fn get_schemas(ds: &Datastore, sess: &Session) -> Result<Vec<TableSchema>, Error> {
    let tables = run_single_statement(&ds, &sess, Statement::Info(InfoStatement::Db), None).await?;

    let mut schemas = match tables {
        Value::Object(o) => TableSchema::from_tables_info_object(o)?,
        t => {
            return Err(Error::UnexpectedType(
                "Object".to_string(),
                format!("{:?}", t),
            ))
        }
    };

    let table_queries = schemas
        .iter()
        .map(|ts| Statement::Info(InfoStatement::Tb(ts.name.as_str().into())))
        .collect::<Vec<_>>();

    let fields = run_statements(&ds, &sess, table_queries, None).await;

    schemas
        .iter_mut()
        .zip(fields)
        .map(|(s, fields)| set_table_schema(s, fields))
        .collect::<Result<_, _>>()?;

    Ok(schemas)
}

fn set_table_schema(
    schema: &mut TableSchema,
    fields: Result<Value, surrealdb::Error>,
) -> Result<(), Error> {
    let fields = fields?;

    let fields = match fields {
        Value::Object(mut o) => o
            .remove("fd")
            .ok_or_else(|| Error::MissingExpectedKey("fd".to_string()))?,
        t => {
            return Err(Error::UnexpectedType(
                "Value::Object".to_string(),
                format!("{:?}", t),
            ))
        }
    };

    let fields = match fields {
        Value::Object(o) => o
            .iter()
            .map(|(_, v)| extract_define_field_from_val(v))
            .collect::<Result<Vec<_>, _>>(),
        t => {
            return Err(Error::UnexpectedType(
                "Value::Object".to_string(),
                format!("{:?}", t),
            ))
        }
    }?;

    schema.fields = fields;

    Ok(())
}

fn extract_define_field_from_val(val: &Value) -> Result<DefineFieldStatement, Error> {
    let define_statement = match parse_to_define_statement(val)? {
        DefineStatement::Field(s) => s,
        t => {
            return Err(Error::UnexpectedType(
                "DefineStatement::Field".to_string(),
                format!("{:?}", t),
            ))
        }
    };

    Ok(define_statement)
}

const IN_MEM_NAMESPACE: &str = "test_namespace";
const IN_MEM_DATABASE: &str = "test_database";

pub async fn in_mem_database() -> Result<(Datastore, Session), Error> {
    let ds = Datastore::new("memory").await?;

    let define_ns_statement =
        Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
            name: IN_MEM_NAMESPACE.into(),
        }));

    run_single_statement(&ds, &Session::for_kv(), define_ns_statement, None).await?;

    let define_db_statement =
        Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
            name: IN_MEM_DATABASE.into(),
        }));

    run_single_statement(
        &ds,
        &Session::for_ns(IN_MEM_NAMESPACE),
        define_db_statement,
        None,
    )
    .await?;

    return Ok((ds, Session::for_db(IN_MEM_NAMESPACE, IN_MEM_DATABASE)));
}

pub async fn apply_migrations_to_in_mem_db(
    migrations: Vec<Statement>,
) -> Result<(Datastore, Session), Error> {
    let (ds, sess) = in_mem_database().await?;

    run_statements(&ds, &sess, migrations, None)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok((ds, sess))
}

pub async fn get_schemas_from_migrations(
    migrations: Vec<Statement>,
) -> Result<Vec<TableSchema>, Error> {
    let (ds, sess) = apply_migrations_to_in_mem_db(migrations).await?;

    get_schemas(&ds, &sess).await
}

fn get_migration_files<P>(directory: P) -> Result<Vec<PathBuf>, Error>
where
    P: AsRef<Path>,
{
    let path = directory.as_ref();
    let mut entries = Vec::default();
    for f in fs::read_dir(path)
        .with_context(|| format!("could not read files in {}", path.to_string_lossy()))?
    {
        let path = f
            .with_context(|| format!("could not read files in {}", path.to_string_lossy()))?
            .path();

        if path.extension().map(|s| s == "sql").unwrap_or_default() {
            entries.push(path);
        }
    }

    entries.sort_unstable();

    Ok(entries)
}

// TODO: make a "migration" struct that wraps a set of statements, and has an optional path so we can point to the source of errors
pub fn read_migrations<P>(directory: P) -> Result<Vec<Statement>, Error>
where
    P: AsRef<Path>,
{
    let mut migrations = Vec::default();

    for f in get_migration_files(directory)? {
        let sql_str = fs::read_to_string(f.as_path())
            .with_context(|| format!("could not read file {}", f.to_string_lossy()))?;
        let parsed_ast = parse(&sql_str)?;
        migrations.extend(parsed_ast.0 .0);
    }

    Ok(migrations)
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use crate::{get_migration_files, get_schemas_from_migrations, read_migrations};

    #[test]
    fn test_get_migration_files() {
        let files = get_migration_files("./examples/migrations").unwrap();

        assert_eq!(files.len(), 2);
        assert!(files[0].to_string_lossy().ends_with("1_create_table.sql"));
        assert!(files[1].to_string_lossy().ends_with("2_drop_col.sql"));
    }

    #[tokio::test]
    async fn test_get_schemas_from_migrations() {
        let migs = read_migrations("./examples/migrations").unwrap();
        let schemas = get_schemas_from_migrations(migs).await.unwrap();

        let schema_str = schemas
            .into_iter()
            .map(|ts| ts.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert_snapshot!(schema_str)
    }
}
