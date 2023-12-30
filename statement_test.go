package squirrelly

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatementBuilder(t *testing.T) {
	sb := StatementBuilder
	query, args, err := sb.Select("test").ToSql()
	assert.Equal(t, "SELECT test", query)
	assert.Empty(t, args)
	assert.Nil(t, err)
}

func TestStatementBuilderPlaceholderFormat(t *testing.T) {
	sb := StatementBuilder.PlaceholderFormat(Dollar)

	query, args, err := sb.Select("test").Where("x = ?").ToSql()
	assert.Equal(t, "SELECT test WHERE x = $1", query)
	assert.Empty(t, args)
	assert.Nil(t, err)
}

func TestStatementBuilderWhere(t *testing.T) {
	sb := StatementBuilder.Where("x = ?", 1)

	sql, args, err := sb.Select("test").Where("y = ?", 2).ToSql()
	assert.NoError(t, err)

	expectedSql := "SELECT test WHERE x = ? AND y = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{1, 2}
	assert.Equal(t, expectedArgs, args)
}
