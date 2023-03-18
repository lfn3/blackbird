use std::collections::BTreeMap;

use blackbird_core::{apply_migrations_to_in_mem_db, read_migrations, run_single_statement, Error};
use blackbird_macros::struct_for;
use surrealdb::sql::{statements::InsertStatement, Data, Object, Statement, Value};

const MIGRATIONS_DIR: &str = "./blackbird/examples/migrations";

struct_for!(Person, "./blackbird/examples/migrations");
const TABLE_NAME: &str = "person";

impl From<&Person> for BTreeMap<String, Value> {
    fn from(value: &Person) -> Self {
        let mut output = BTreeMap::new();
        if let Some(name) = value.name.as_ref() {
            output.insert("name".to_string(), Value::Strand(name.as_str().into()));
        }
        output.insert(
            "username".to_string(),
            Value::Strand(value.username.as_str().into()),
        );

        output
    }
}

impl Person {
    fn insert_statement(&self) -> Statement {
        Statement::Insert(InsertStatement {
            into: TABLE_NAME.into(),
            data: Data::SingleExpression(Value::Object(Object(self.into()))),
            ..InsertStatement::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let migrations = read_migrations(MIGRATIONS_DIR)?;
    let (ds, sess) = apply_migrations_to_in_mem_db(migrations).await?;

    let p = Person {
        name: Some("bob".to_string()),
        username: "b0b".to_string(),
    };

    let insert_val = run_single_statement(&ds, &sess, p.insert_statement(), None).await?;

    println!("insert_val: {insert_val:#?}");
    // Something like:
    // Value::Array(Array(vec![Value::Object(Object(
    //     vec![
    //         (
    //             "id".to_string(),
    //             Value::Thing(Thing {
    //                 tb: "person".to_string(),
    //                 id: "byszviff58xptd5v6f6b".into(),  // This does change!
    //             }),
    //         ),
    //         ("name".to_string(), Value::Strand("bob".into())),
    //     ]
    //     .into_iter()
    //     .collect(),
    // ))]));

    Ok(())
}
