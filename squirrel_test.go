package squirrelly

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var sqlizer = Select("test")
var sqlStr = "SELECT test"

var testDebugUpdateSQL = Update("table").SetMap(Eq{"x": 1, "y": "val"})
var expectedDebugUpateSQL = "UPDATE table SET x = '1', y = 'val'"

func TestDebugSqlizerUpdateColon(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

func TestDebugSqlizerUpdateAtp(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

func TestDebugSqlizerUpdateDollar(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

func TestDebugSqlizerUpdateQuestion(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugUpateSQL, DebugSqlizer(testDebugUpdateSQL))
}

var testDebugDeleteSQL = Delete("table").Where(And{
	Eq{"column": "val"},
	Eq{"other": 1},
})
var expectedDebugDeleteSQL = "DELETE FROM table WHERE (column = 'val' AND other = '1')"

func TestDebugSqlizerDeleteColon(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

func TestDebugSqlizerDeleteAtp(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

func TestDebugSqlizerDeleteDollar(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

func TestDebugSqlizerDeleteQuestion(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugDeleteSQL, DebugSqlizer(testDebugDeleteSQL))
}

var testDebugInsertSQL = Insert("table").Values(1, "test")
var expectedDebugInsertSQL = "INSERT INTO table VALUES ('1','test')"

func TestDebugSqlizerInsertColon(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

func TestDebugSqlizerInsertAtp(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

func TestDebugSqlizerInsertDollar(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

func TestDebugSqlizerInsertQuestion(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugInsertSQL, DebugSqlizer(testDebugInsertSQL))
}

var testDebugSelectSQL = Select("*").From("table").Where(And{
	Eq{"column": "val"},
	Eq{"other": 1},
})
var expectedDebugSelectSQL = "SELECT * FROM table WHERE (column = 'val' AND other = '1')"

func TestDebugSqlizerSelectColon(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizerSelectAtp(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizerSelectDollar(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizerSelectQuestion(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugSelectSQL, DebugSqlizer(testDebugSelectSQL))
}

func TestDebugSqlizer(t *testing.T) {
	sqlizer := Expr("x = ? AND y = ? AND z = '??'", 1, "text")
	expectedDebug := "x = '1' AND y = 'text' AND z = '?'"
	assert.Equal(t, expectedDebug, DebugSqlizer(sqlizer))
}

func TestDebugSqlizerErrors(t *testing.T) {
	errorMsg := DebugSqlizer(Expr("x = ?", 1, 2)) // Not enough placeholders
	assert.True(t, strings.HasPrefix(errorMsg, "[DebugSqlizer error: "))

	errorMsg = DebugSqlizer(Expr("x = ? AND y = ?", 1)) // Too many placeholders
	assert.True(t, strings.HasPrefix(errorMsg, "[DebugSqlizer error: "))

	errorMsg = DebugSqlizer(Lt{"x": nil}) // Cannot use nil values with Lt
	assert.True(t, strings.HasPrefix(errorMsg, "[ToSql error: "))
}
