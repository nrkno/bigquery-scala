# Bigquery library for scala

## Motivation

SQL queries (or programs) for BigQuery may be hard to maintain over time. This scala library focusing on 
adding a higher-level API with focus on correctness, maintainability and testability. It's heavily inspired by
doobie which is a pure functional abstraction over JDBC.  

What does this library include:
- A scala DSL where we can construct BigQuery SQL.
- A test rig that gives the developer fast feedback using snapshot testing.
- Documentation of the generated through snapshot testing.
- Deploy, upgrade and maintain table, views and UDFs.


## How do I use it?

- [Setup sbt](./docs/sbt_setup.md)
- [Query example](./docs/example_query.md)
- [View example](./docs/example_view.md)
- [User defined function (UDF) example](./docs/example_udf.md)
- [Table value function (TVF) exampe](./docs/example_tvf.md)
- [Rewrite structs](./docs/struct_projection.md)