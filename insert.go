package squirrelly

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
	"github.com/lann/builder"
)

type insertData struct {
	PlaceholderFormat PlaceholderFormat
	Prefixes          []Sqlizer
	StatementKeyword  string
	Options           []string
	Into              string
	Columns           []string
	Values            [][]interface{}
	Suffixes          []Sqlizer
	Select            *SelectBuilder
	ConflictKeys      []string
	UpdateColumns     []string
	DoNothing         bool
	Returning         []string
}

func (d *insertData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.Into) == 0 {
		err = errors.New("insert statements must specify a table")
		return
	}
	if len(d.Values) == 0 && d.Select == nil {
		err = errors.New("insert statements must have at least one set of values or select clause")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, err = appendToSql(d.Prefixes, sql, " ", args)
		if err != nil {
			return
		}

		sql.WriteString(" ")
	}

	if d.StatementKeyword == "" {
		sql.WriteString("INSERT ")
	} else {
		sql.WriteString(d.StatementKeyword)
		sql.WriteString(" ")
	}

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(d.Into)
	sql.WriteString(" ")

	if len(d.Columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(d.Columns, ","))
		sql.WriteString(") ")
	}

	if d.Select != nil {
		args, err = d.appendSelectToSQL(sql, args)
	} else {
		args, err = d.appendValuesToSQL(sql, args)
	}
	if err != nil {
		return
	}

	if len(d.ConflictKeys) > 0 {
		sql.WriteString(" ON CONFLICT (")
		sql.WriteString(strings.Join(d.ConflictKeys, ","))
		sql.WriteString(")")

		if d.DoNothing {
			if len(d.UpdateColumns) > 0 {
				err = errors.New("insert statements with OnConflict can't use both UpdateColumns and DoNothing")
				return
			}

			sql.WriteString(" DO NOTHING")
		} else {
			if len(d.UpdateColumns) > 0 {
				sql.WriteString(" DO UPDATE SET")
				for idx, col := range d.UpdateColumns {
					if idx != 0 {
						sql.WriteString(",")
					}

					sql.WriteString(fmt.Sprintf(" %[1]s = EXCLUDED.%[1]s", col))
				}
			} else {
				err = errors.New("insert statements with OnConflict set must have at least one column to be updated")
				return
			}
		}
	}

	if len(d.Returning) > 0 {
		sql.WriteString(" RETURNING ")
		sql.WriteString(strings.Join(d.Returning, ","))
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(d.Suffixes, sql, " ", args)
		if err != nil {
			return
		}
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

func (d *insertData) appendValuesToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if len(d.Values) == 0 {
		return args, errors.New("values for insert statements are not set")
	}

	io.WriteString(w, "VALUES ")

	valuesStrings := make([]string, len(d.Values))
	for r, row := range d.Values {
		valueStrings := make([]string, len(row))
		for v, val := range row {
			if vs, ok := val.(Sqlizer); ok {
				vsql, vargs, err := vs.ToSql()
				if err != nil {
					return nil, err
				}
				valueStrings[v] = vsql
				args = append(args, vargs...)
			} else {
				valueStrings[v] = "?"
				args = append(args, val)
			}
		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}

	io.WriteString(w, strings.Join(valuesStrings, ","))

	return args, nil
}

func (d *insertData) appendSelectToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if d.Select == nil {
		return args, errors.New("select clause for insert statements are not set")
	}

	selectClause, sArgs, err := d.Select.ToSql()
	if err != nil {
		return args, err
	}

	io.WriteString(w, selectClause)
	args = append(args, sArgs...)

	return args, nil
}

// Builder

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder builder.Builder

func init() {
	builder.Register(InsertBuilder{}, insertData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b InsertBuilder) PlaceholderFormat(f PlaceholderFormat) InsertBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(InsertBuilder)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b InsertBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(insertData)
	return data.ToSql()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b InsertBuilder) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b InsertBuilder) Prefix(sql string, args ...interface{}) InsertBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b InsertBuilder) PrefixExpr(expr Sqlizer) InsertBuilder {
	return builder.Append(b, "Prefixes", expr).(InsertBuilder)
}

// Options adds keyword options before the INTO clause of the query.
func (b InsertBuilder) Options(options ...string) InsertBuilder {
	return builder.Extend(b, "Options", options).(InsertBuilder)
}

// Into sets the INTO clause of the query.
func (b InsertBuilder) Into(from string) InsertBuilder {
	return builder.Set(b, "Into", from).(InsertBuilder)
}

// Columns adds insert columns to the query.
func (b InsertBuilder) Columns(columns ...string) InsertBuilder {
	return builder.Extend(b, "Columns", columns).(InsertBuilder)
}

// Values adds a single row's values to the query.
func (b InsertBuilder) Values(values ...interface{}) InsertBuilder {
	return builder.Append(b, "Values", values).(InsertBuilder)
}

// Suffix adds an expression to the end of the query
func (b InsertBuilder) Suffix(sql string, args ...interface{}) InsertBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b InsertBuilder) SuffixExpr(expr Sqlizer) InsertBuilder {
	return builder.Append(b, "Suffixes", expr).(InsertBuilder)
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b InsertBuilder) SetMap(clauses map[string]interface{}) InsertBuilder {
	// Keep the columns in a consistent order by sorting the column key string.
	cols := make([]string, 0, len(clauses))
	for col := range clauses {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	vals := make([]interface{}, 0, len(clauses))
	for _, col := range cols {
		vals = append(vals, clauses[col])
	}

	b = builder.Set(b, "Columns", cols).(InsertBuilder)
	b = builder.Set(b, "Values", [][]interface{}{vals}).(InsertBuilder)

	return b
}

// OnConflict is used to turn an insert into an upsert. This is used to add the ON CONFLICT (keys ...) clause. When used with [InsertBuilder.UpdateColumns] the insert builder adds ON CONFLICT (keys ...) DO UPDATE SET ....
func (b InsertBuilder) OnConflict(conflictKeys ...string) InsertBuilder {
	return builder.Extend(b, "ConflictKeys", conflictKeys).(InsertBuilder)
}

// DoNothing, when used with [InsertBuilder.OnConflict], generates ON CONFLICT DO NOTHING clause to the insert builder.
func (b InsertBuilder) DoNothing() InsertBuilder {
	return builder.Set(b, "DoNothing", true).(InsertBuilder)
}

// UpdateColumns, when used with [InsertBuilder.OnConflict], generates ON CONFLICT DO UPDATE clauses to the insert builder.
func (b InsertBuilder) UpdateColumns(columns ...string) InsertBuilder {
	return builder.Extend(b, "UpdateColumns", columns).(InsertBuilder)
}

// Returning adds a RETURNING <columns> suffix (before the [InsertBuilder.Suffix]) to the insert builder.
func (b InsertBuilder) Returning(columns ...string) InsertBuilder {
	return builder.Extend(b, "Returning", columns).(InsertBuilder)
}

// Select set Select clause for insert query
// If Values and Select are used, then Select has higher priority
func (b InsertBuilder) Select(sb SelectBuilder) InsertBuilder {
	return builder.Set(b, "Select", &sb).(InsertBuilder)
}

func (b InsertBuilder) statementKeyword(keyword string) InsertBuilder {
	return builder.Set(b, "StatementKeyword", keyword).(InsertBuilder)
}

// StructValues sets values for an insert builder based on the columns already set and the "sq" tag on the struct's value.
// For example
//
//	s := struct{A `sq:"a"`}{A: "foo"}
//	Insert().Columns("a").StructValues(&s)
//
//	// is equivalent to
//	Insert().Columns("a").Values(s.A)
func (b InsertBuilder) StructValues(data interface{}) InsertBuilder {
	if data == nil {
		return b
	}

	mapper := getMapper()
	lookup := mapper.FieldMap(reflect.ValueOf(data))

	rawColumns, _ := builder.Get(b, "Columns")
	columns := rawColumns.([]string)

	values := make([]interface{}, len(columns))
	for idx, columnName := range columns {
		value, hasValue := lookup[columnName]
		if !hasValue {
			panic(fmt.Errorf("missing column `%[1]s` in struct. Is it tagged with `sq:\"%[1]s\"`?", columnName))
		}

		values[idx] = value.Interface()
	}

	return b.Values(values...)
}

// Struct sets the columns and values for an insert builder based on a struct's tags
//
// For example
//
//	type Foo struct {
//		A string `sq:"a"`
//		B string `sq:"column_b"`
//	}
//
//	record := Foo{A: "a value", B: "another value"}
//
//	// these lines have the same result
//	Insert("table").Struct(&record)
//	Insert("table").Columns("a", "b").StructValues(&record)
func (b InsertBuilder) Struct(data interface{}) InsertBuilder {
	if data == nil {
		return b
	}

	mapper := reflectx.NewMapper("sq")
	lookup := mapper.FieldMap(reflect.ValueOf(data))

	columns := make([]string, len(lookup))
	values := make([]interface{}, len(lookup))

	idx := 0
	for column, value := range lookup {
		columns[idx] = column
		values[idx] = value.Interface()

		idx += 1
	}

	return b.Columns(columns...).Values(values...)
}
