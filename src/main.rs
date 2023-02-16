use std::collections::BTreeMap;

use surrealdb::{
    sql::{
        parse,
        statements::{
            DefineDatabaseStatement, DefineFieldStatement, DefineNamespaceStatement,
            DefineStatement, DefineTableStatement, InsertStatement, SelectStatement,
        },
        Data, Field, Fields, Idiom, Object, Part, Query, Statement, Statements, Table, Value,
        Values,
    },
    Datastore, Session,
};

#[derive(thiserror::Error, Debug)]
enum Error {
    #
    DbError(surrealdb::Error),
}

async fn run_single_statement(
    ds: &Datastore,
    sess: &Session,
    query: Statement,
    vars: Option<BTreeMap<String, Value>>,
) -> Result<Response, Error> {
    ds.process(Query(Statements(vec![query])), sess, vars, true)
        .await
}

#[tokio::main]
async fn main() {
    let ds = Datastore::new("memory").await.unwrap();

    let namespace = "test_namespace";

    let define_ns_query = Query(Statements(vec![Statement::Define(
        DefineStatement::Namespace(DefineNamespaceStatement {
            name: namespace.into(),
        }),
    )]));

    ds.process(define_ns_query, &Session::for_kv(), None, true)
        .await
        .unwrap();

    let database = "test_database";

    let define_db_query = Query(Statements(vec![Statement::Define(
        DefineStatement::Database(DefineDatabaseStatement {
            name: database.into(),
        }),
    )]));

    ds.process(define_db_query, &Session::for_ns(namespace), None, true)
        .await
        .unwrap();

    let sess = Session::for_db(namespace, database);

    let table_name = "person";

    let define_table_query = Query(Statements(vec![
        Statement::Define(DefineStatement::Table(DefineTableStatement {
            name: table_name.into(),
            ..DefineTableStatement::default()
        })),
        Statement::Define(DefineStatement::Field(DefineFieldStatement {
            name: Idiom(vec![Part::Field("name".into())]),
            what: table_name.into(),
            kind: Some(surrealdb::sql::Kind::String),
            ..DefineFieldStatement::default()
        })),
    ]));

    ds.process(define_table_query, &sess, None, true)
        .await
        .unwrap();

    let mut obj = BTreeMap::new();
    obj.insert("name".to_string(), Value::Strand("bob".into()));

    let insert_person = Query(Statements(vec![Statement::Insert(InsertStatement {
        into: table_name.into(),
        data: Data::SingleExpression(Value::Object(Object(obj))),
        ..InsertStatement::default()
    })]));

    ds.process(insert_person, &sess, None, true).await.unwrap();

    let ast = Query(Statements(vec![Statement::Select(SelectStatement {
        expr: Fields(vec![Field::All]),
        what: Values(vec![Value::Table(Table("person".to_string()))]),
        ..Default::default()
    })]));

    println!("constructed ast: {ast:#?}");

    let parsed_ast = parse("SELECT * FROM person;").unwrap();
    println!("parsed ast: {parsed_ast:#?}");

    let select_res = ds.process(ast, &sess, None, true).await.unwrap();
    println!("resp: {:#?}", select_res);
}
