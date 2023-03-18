use surrealdb::sql::parse;

fn main() {
    let sql = r#"DEFINE FIELD name ON person TYPE string ASSERT $value != NONE;"#;
    let parsed_ast = parse(sql).unwrap();
    println!("{sql} was parsed to: {parsed_ast:#?}");
}
