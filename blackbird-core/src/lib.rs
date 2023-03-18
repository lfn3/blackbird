use std::{
    collections::BTreeMap,
    fs::{self},
    path::{Path, PathBuf},
};

use surrealdb::{
    sql::{
        parse,
        statements::{DefineDatabaseStatement, DefineNamespaceStatement, DefineStatement},
        Query, Statement, Statements, Value,
    },
    Datastore, Session,
};

pub mod schema;

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

const IN_MEM_NAMESPACE: &str = "test_namespace";
const IN_MEM_DATABASE: &str = "test_database";

pub async fn in_mem_database() -> Result<(Datastore, Session), Error> {
    let ds = Datastore::new("memory").await?;
    let sess = create_db_and_ns(&ds, IN_MEM_NAMESPACE, IN_MEM_DATABASE).await?;
    return Ok((ds, sess));
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

pub async fn create_db_and_ns(
    datastore: &Datastore,
    namespace: &str,
    database: &str,
) -> Result<Session, Error> {
    let define_ns_statement =
        Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
            name: namespace.into(),
        }));

    run_single_statement(datastore, &Session::for_kv(), define_ns_statement, None).await?;

    let define_db_statement =
        Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
            name: database.into(),
        }));

    run_single_statement(
        datastore,
        &Session::for_ns(namespace),
        define_db_statement,
        None,
    )
    .await?;

    Ok(Session::for_db(namespace, database))
}

#[cfg(test)]
mod tests {
    use crate::get_migration_files;

    #[test]
    fn test_get_migration_files() {
        let files = get_migration_files("../blackbird/examples/migrations").unwrap();

        assert_eq!(files.len(), 3);
        assert!(files[0].to_string_lossy().ends_with("1_create_table.sql"));
        assert!(files[1].to_string_lossy().ends_with("2_drop_col.sql"));
        assert!(files[1]
            .to_string_lossy()
            .ends_with("3_add_not_null_col.sql"));
    }
}
