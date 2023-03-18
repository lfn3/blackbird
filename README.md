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

### TODO:

- [ ] Figure out an approach.
      Given there's a required pk, (`id`) an ORM style might actually be viable?
      But I do really like the idea of [cornucopia](https://github.com/cornucopia-rs/cornucopia) where we create functions for calling queries.
      I think I probably just need to try to function gen approach?
- [ ] Make `struct_for!` accept a const str as a migration path
- [ ] How should we handle the special `id` column?
- [ ] What impls should we generate on our structs?
- [x] Prototype nullabilty handling
