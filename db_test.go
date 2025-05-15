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

	singleScanQuery := sq.Select("comment").From("foo").Where(sq.Eq{"pk": 10})
	var comment string
	assert.NoError(t, db.Get(singleScanQuery, &comment))
	assert.Equal(t, comment, "another comment")
}

func TestDbGetTextPk(t *testing.T) {
	db, _ := sq.Open("sqlite", "file::memory:")

	_, err := db.DB.Exec("CREATE TABLE foo (bar TEXT PRIMARY KEY)")
	assert.NoError(t, err)

	type foo struct {
		Bar string `sq:"bar"`
	}

	insert := sq.Insert("foo").Columns("bar").StructValues(&foo{Bar: "this is some text"})
	_, err = db.Exec(insert)
	assert.NoError(t, err)

	records := []*foo{}
	getAllQuery := sq.Select("*").From("foo").OrderBy("bar ASC")
	assert.NoError(t, db.GetAll(getAllQuery, &records))
	assert.Equal(t, records, []*foo{&foo{Bar: "this is some text"}})
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

	assert.NoError(t, db.WithTx(func(tx sq.DbLike) error {
		insert := sq.Insert("foo").Columns("pk", "comment").StructValues(&foo{Pk: 77, Comment: "lucky numbers sevens"})
		_, err := tx.Exec(insert)
		return err
	}))

	assert.NoError(t, db.GetAll(getAllQuery, &records))
	assert.Equal(t, records, []*foo{
		&foo{Pk: 1, Comment: "comment"},
		&foo{Pk: 77, Comment: "lucky numbers sevens"},
	})
}

func TestDbGetMap(t *testing.T) {
	db, _ := sq.Open("sqlite", "file::memory:")

	_, err := db.DB.Exec("CREATE TABLE foo (pk INTEGER PRIMARY KEY AUTOINCREMENT, comment TEXT NOT NULL)")
	assert.NoError(t, err)

	type foo struct {
		Pk      int    `sq:"pk"`
		Comment string `sq:"comment"`
	}

	insert := sq.Insert("foo").Columns("comment").Values("first").Values("second").Values("third").Values("fourth")
	_, err = db.Exec(insert)
	assert.NoError(t, err)

	query := sq.Select("*").From("foo")
	records, err := sq.DbGetMap[int, *foo](db, query, "pk")
	assert.NoError(t, err)

	assert.Equal(t, records, map[int]*foo{
		1: {Pk: 1, Comment: "first"},
		2: {Pk: 2, Comment: "second"},
		3: {Pk: 3, Comment: "third"},
		4: {Pk: 4, Comment: "fourth"},
	})

	_, err = db.Exec(insert)
	assert.NoError(t, err)

	recSlice, err := sq.DbGetMap[string, []*foo](db, query, "comment")
	assert.NoError(t, err)

	assert.Equal(t, recSlice, map[string][]*foo{
		"first": {
			{Pk: 1, Comment: "first"},
			{Pk: 5, Comment: "first"},
		},
		"second": {
			{Pk: 2, Comment: "second"},
			{Pk: 6, Comment: "second"},
		},
		"third": {
			{Pk: 3, Comment: "third"},
			{Pk: 7, Comment: "third"},
		},
		"fourth": {
			{Pk: 4, Comment: "fourth"},
			{Pk: 8, Comment: "fourth"},
		},
	})
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
