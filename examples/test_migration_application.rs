use blackbird::{get_schemas_from_migrations, read_migrations, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let migs = read_migrations("./examples/migrations")?;
    let schemas = get_schemas_from_migrations(migs).await?;

    for s in schemas {
        println!("{s}");
    }

    Ok(())
}
