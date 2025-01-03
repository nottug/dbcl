# DBCL

This is a small MySQL wrapper for [sqlx](https://github.com/jmoiron/sqlx). `sqlx` is extremely
useful for wrapping Go's native `database/sql` package, but it is explicitly designed to
be fairly low-level. This library is a collection of common helpers I've come to use in most
projects using MySQL.

There are three main components

 1. `querier`: one big pain point is allowing standard database connections and
 	database transactions to use the same functions. `querier` creates a standard
 	interface to make that simple
 2. `migrations`: dead simple migrations using go:embed to store SQL files
 3. `query builders`: a few helpers for building more complex insert statements

There are a few other things like `NullBigInt`, read+write helpers, and simple
conversion functions, but those are less useful than the above three.