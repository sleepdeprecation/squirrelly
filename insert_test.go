package squirrelly

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL :=
		"WITH prefix AS ? " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES (?,?),(?,? + 1) " +
			"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSql()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSql()
	assert.Error(t, err)
}

func TestInsertBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestInsertBuilderMustSql should have panicked!")
		}
	}()
	Insert("").MustSql()
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"field1": 1, "field2": 2, "field3": 3})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table (field1,field2,field3) VALUES (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSelect(t *testing.T) {
	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReplace(t *testing.T) {
	b := Replace("table").Values(1)

	expectedSQL := "REPLACE INTO table VALUES (?)"

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
}

func TestInsertBuilderOnConflictUpdateColumns(t *testing.T) {
	ib := Insert("table").Columns("field1", "field2", "field3").Values("one", "two", "three").OnConflict("field1").UpdateColumns("field2", "field3")

	expectedSql := "INSERT INTO table (field1,field2,field3) VALUES (?,?,?) ON CONFLICT (field1) DO UPDATE SET field2 = EXCLUDED.field2, field3 = EXCLUDED.field3"

	sql, _, err := ib.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, expectedSql, sql)
}

func TestInsertStructValues(t *testing.T) {
	record := struct {
		Pk      int    `sq:"pk"`
		Comment string `sq:"comment"`
	}{
		Pk:      1,
		Comment: "foo",
	}

	ib := Insert("table").Columns("pk", "comment").StructValues(&record)

	expectedSql := "INSERT INTO table (pk,comment) VALUES (?,?)"
	expectedArgs := []interface{}{1, "foo"}

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, expectedSql, sql)
	assert.ElementsMatch(t, expectedArgs, args)
}

func TestInsertStructValuesNotMapped(t *testing.T) {
	record := struct {
		Pk      int `sq:"pk"`
		Comment string
	}{
		Pk:      1,
		Comment: "foo",
	}

	assert.PanicsWithError(
		t,
		"missing column `comment` in struct. Is it tagged with `sq:\"comment\"`?",
		func() {
			Insert("table").Columns("pk", "comment").StructValues(&record)
		},
	)
}

func TestInsertStruct(t *testing.T) {
	record := struct {
		Pk      int    `sq:"pk"`
		Comment string `sq:"comment"`
	}{
		Pk:      1,
		Comment: "foo",
	}

	ib := Insert("table").Struct(&record)

	expectedSql := "INSERT INTO table (pk,comment) VALUES (?,?)"
	expectedArgs := []interface{}{1, "foo"}

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, expectedSql, sql)
	assert.ElementsMatch(t, expectedArgs, args)
}
