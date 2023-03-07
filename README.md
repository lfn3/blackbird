Hacky as all get out, less than minimally viable, opinionated, proof of concept that generates rust structs from a set of [SurrealDB](https://github.com/surrealdb/surrealdb) migrations.    
    
```
use blackbird_macros::struct_for;

struct_for!(Person, "./blackbird/examples/migrations");
```

Expands to:

```
struct Person {
    pub name: String,
}
```

Given the migrations in the [example dir](https://github.com/lfn3/blackbird/tree/main/blackbird/examples/migrations)