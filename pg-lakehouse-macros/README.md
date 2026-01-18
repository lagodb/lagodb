# pg-lakehouse-macros

**Procedural macros for the pg-lakehouse framework.**

This crate provides the macro support for `pg-lakehouse-core`. Its primary purpose is to reduce the boilerplate required to register custom storage handlers in PostgreSQL.

## Features

- **`#[pg_table_am]`**: Automatically generates the `_PG_init` hooks and registration logic for a `TableAccessMethod` implementation. It handles the conversion of Rust traits into the `TableAmRoutine` structure expected by PostgreSQL's C API.

## Usage

This crate is intended to be used as a dependency of `pg-lakehouse-core` and is re-exported there. Most users should simply use `pg_lakehouse_core::pg_table_am`.

```rust
use pg_lakehouse_core::prelude::*;

#[pg_table_am(
    version = "0.1.0",
    author = "Robert Mu",
    website = "https://github.com/robertmu/pg-lakehouse"
)]
pub struct MyCustomAm;

// Implement TableAccessMethod for MyCustomAm...
```

## License

Apache License 2.0.
