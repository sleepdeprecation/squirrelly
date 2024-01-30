package squirrelly_test

import (
	"fmt"
	"testing"

	sq "github.com/sleepdeprecation/squirrelly"
	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

func TestDbGet(t *testing.T) {
	db, _ := sq.Open("sqlite", "file::memory:")

	_, err := db.DB.Exec("CREATE TABLE foo (pk INTEGER PRIMARY KEY, comment TEXT NOT NULL)")
	assert.NoError(t, err)

	type foo struct {
		Pk      int    `sq:"pk"`
		Comment string `sq:"comment"`
	}

	insert := sq.Insert("foo").Columns("pk", "comment").StructValues(&foo{Pk: 1, Comment: "comment"})
	_, err = db.Exec(insert)
	assert.NoError(t, err)

	_, err = db.Exec(sq.Insert("foo").Columns("pk", "comment").StructValues(&foo{Pk: 10, Comment: "another comment"}))
	assert.NoError(t, err)

	records := []*foo{}
	getAllQuery := sq.Select("*").From("foo")
	assert.NoError(t, db.GetAll(getAllQuery, &records))
	assert.Equal(t, records, []*foo{&foo{Pk: 1, Comment: "comment"}, &foo{Pk: 10, Comment: "another comment"}})

	record := &foo{}
	query := sq.Select("*").From("foo").Where(sq.Eq{"pk": 1})
	assert.NoError(t, db.Get(query, record))
	assert.Equal(t, record, &foo{Pk: 1, Comment: "comment"})
}

func TestTx(t *testing.T) {
	db, _ := sq.Open("sqlite", "file::memory:")

	_, err := db.DB.Exec("CREATE TABLE foo (pk INTEGER PRIMARY KEY, comment TEXT NOT NULL)")
	assert.NoError(t, err)

	type foo struct {
		Pk      int    `sq:"pk"`
		Comment string `sq:"comment"`
	}

	tx, err := db.Begin()
	assert.NoError(t, err)

	insert := sq.Insert("foo").Columns("pk", "comment").StructValues(&foo{Pk: 1, Comment: "comment"})
	_, err = tx.Exec(insert)
	assert.NoError(t, err)

	assert.NoError(t, tx.Commit())

	records := []*foo{}
	getAllQuery := sq.Select("*").From("foo")
	assert.NoError(t, db.GetAll(getAllQuery, &records))
	assert.Equal(t, records, []*foo{&foo{Pk: 1, Comment: "comment"}})
}

func ExampleDb() {
	// open a sqlite database in memory
	db, _ := sq.Open("sqlite", "file::memory:")

	// initialize a basic table
	// you can use db.DB to access the *sqlx.DB methods directly
	// (these match the *sql.Db methods, with some additions).
	db.DB.Exec("CREATE TABLE example (pk INTEGER PRIMARY KEY AUTOINCREMENT, comment TEXT NOT NULL)")

	type Example struct {
		Pk      int    `sq:"pk"`
		Comment string `sq:"comment"`
	}

	// build some queries
	insert := sq.Insert("example").Columns("comment").Values("first").Values("second").Values("third")
	_, err := db.Exec(insert)
	if err != nil {
		panic(err)
	}

	values := []*Example{}
	query := sq.Select("*").From("example")
	err = db.GetAll(query, &values)

	fmt.Printf("%+v\n", values)
}
