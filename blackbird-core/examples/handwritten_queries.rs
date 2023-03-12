use std::collections::BTreeMap;

use blackbird_core::{get_schemas, run_single_statement, run_statements, Error};
use surrealdb::{
    sql::{
        statements::{
            DefineDatabaseStatement, DefineFieldStatement, DefineNamespaceStatement,
            DefineStatement, DefineTableStatement, InsertStatement, SelectStatement,
        },
        Data, Field, Fields, Idiom, Object, Part, Query, Statement, Statements, Table, Value,
        Values,
    },
    Datastore, Session,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let ds = Datastore::new("memory").await?;

    let namespace = "test_namespace";

    let define_ns_statement =
        Statement::Define(DefineStatement::Namespace(DefineNamespaceStatement {
            name: namespace.into(),
        }));

    run_single_statement(&ds, &Session::for_kv(), define_ns_statement, None).await?;

    let database = "test_database";

    let define_db_statement =
        Statement::Define(DefineStatement::Database(DefineDatabaseStatement {
            name: database.into(),
        }));

    run_single_statement(&ds, &Session::for_ns(namespace), define_db_statement, None).await?;

    let sess = Session::for_db(namespace, database);

    let table_name = "person";

    let define_table_statements = vec![
        Statement::Define(DefineStatement::Table(DefineTableStatement {
            name: table_name.into(),
            full: true,
            ..DefineTableStatement::default()
        })),
        Statement::Define(DefineStatement::Field(DefineFieldStatement {
            name: Idiom(vec![Part::Field("name".into())]),
            what: table_name.into(),
            kind: Some(surrealdb::sql::Kind::String),
            ..DefineFieldStatement::default()
        })),
    ];

    run_statements(
        &ds,
        &Session::for_db(namespace, database),
        define_table_statements,
        None,
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;

    let mut obj = BTreeMap::new();
    obj.insert("name".to_string(), Value::Strand("bob".into()));

    let insert_person = Query(Statements(vec![Statement::Insert(InsertStatement {
        into: table_name.into(),
        data: Data::SingleExpression(Value::Object(Object(obj))),
        ..InsertStatement::default()
    })]));

    ds.process(insert_person, &sess, None, true).await?;

    let select_statement = Statement::Select(SelectStatement {
        expr: Fields(vec![Field::All]),
        what: Values(vec![Value::Table(Table("person".to_string()))]),
        ..Default::default()
    });

    let select_res = run_single_statement(&ds, &sess, select_statement, None).await?;
    println!("resp: {:#?}", select_res);

    let info_res = get_schemas(&ds, &sess).await?;

    println!("Schemas:");
    for ts in info_res {
        println!("{ts}");
    }

    Ok(())
}
