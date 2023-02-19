use std::{collections::BTreeMap, fmt::Display};

use surrealdb::{
    sql::{
        parse,
        statements::{DefineFieldStatement, DefineStatement, DefineTableStatement, InfoStatement},
        Object, Query, Statement, Statements, Value,
    },
    Datastore, Session,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    DbError(surrealdb::Error),

    #[error("Expected {0} rows, got {1}")]
    UnexpectedResultCount(usize, usize),

    #[error("Expected type {0}, but got {1}")]
    UnexpectedType(String, String),

    #[error("Object is missing expected key: {0}")]
    MissingExpectedKey(String),
}

impl From<surrealdb::Error> for Error {
    fn from(value: surrealdb::Error) -> Self {
        return Error::DbError(value);
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

#[derive(Debug)]
pub struct TableSchema {
    name: String,
    definition: DefineTableStatement,
    fields: Vec<DefineFieldStatement>,
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
