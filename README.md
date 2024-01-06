# Squirrelly - Squirrel with better struct support

The most common database operations are converting structs into database records and turning database records into structs.
The goal of Squirrelly is to take the wonderful fluent sql builder of [Squirrel](https://github.com/Masterminds/squirrel) and add struct to record and record to struct mappings.

[![CoDoc](https://pkg.go.dev/badge/github.com/sleepdeprecation/squirrelly.svg)](https://pkg.go.dev/github.com/sleepdeprecation/squirrelly)

``` go
import (
    sq "github.com/sleepdeprecation/squirrelly"
    _ "modernc.org/sqlite"
)

type Comment struct {
    Id int `sq:"id"`
    Comment string `sq:"comment"`
    CommenterEmail string `sq:"commenter_email"`
}

func main() {
    db, err := sq.Open("sqlite", "file::memory:")

	// you can use db.DB to access the *sqlx.DB functions directly
	// (these match the *sql.Db functions, with some additions).
	db.DB.Exec("CREATE TABLE example (pk INTEGER PRIMARY KEY AUTOINCREMENT, comment TEXT NOT NULL)")

    db.Exec(sq.Insert("example").Columns("comment", "commenter_email").StructValues(&Comment{
        Comment: "my fun comment",
        CommenterEmail: "foo@example.com",
    }))

    record := Comment{}
    err = db.Get(sq.Select("*").From("example").Where(sq.Eq{"commenter_email": "foo@example.com"}, &record).Limit(1))
    // record should now equal
    //  Comment{Comment: "my fun comment", CommenterEmail: "foo@example.com"}

    records := []*Comment{}
    err = db.GetAll(sq.Select("*").From("example"), &records)
}
```

Check out the [original Squirrel README](https://github.com/Masterminds/squirrel) for more information on how the SQL generator works.


## Changes from Squirrel

Squirrelly is lacking several features from the original Squirrel, specifically the `RunWith`, `Query`, and `Exec` functions have been removed from the query builders themselves.
In their place, Squirrelly uses a separate database abstraction to handle the queries.

As Squirrelly is more focused on mapping structs and database records together, it adds a new `StructValues` function to the `InsertBuilder`, which directly maps structs into insert values.
