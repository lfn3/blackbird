use surrealdb::sql::parse;

fn main() {
    let sql = r#"SELECT * FROM person;"#;
    let parsed_ast = parse(sql).unwrap();
    println!("{sql} was parsed to: {parsed_ast:#?}");
}
