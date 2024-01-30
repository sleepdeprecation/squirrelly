package squirrelly

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx/reflectx"
)

// Type Db is a wrapper around sql.Db
type Db struct {
	*sql.DB
}

type Tx struct {
	*sql.Tx
}

// interface for database/sql db like structs
type Querier interface {
	Exec(string, ...any) (sql.Result, error)
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
}

// interface for squirrelly db like structs
type DbLike interface {
	Exec(Sqlizer) (sql.Result, error)
	Query(Sqlizer) (*sql.Rows, error)
	QueryRow(Sqlizer) *sql.Row
	Get(Sqlizer, any) error
	GetAll(Sqlizer, any) error
}

// Open uses the same convention as [database/sql.Open], a driver name and a source string, both dependant on your driver's package.
func Open(driver, source string) (*Db, error) {
	sqldb, err := sql.Open(driver, source)
	if err != nil {
		return nil, err
	}

	return &Db{sqldb}, nil
}

func (db *Db) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}

	return &Tx{tx}, nil
}

func (db *Db) WithTx(fn func(DbLike) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	err = fn(tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Exec runs [database/sql.DB.Exec], using a squirrelly builder.
func Exec(db Querier, query Sqlizer) (sql.Result, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	return db.Exec(sql, args...)
}

// Exec runs [database/sql.DB.Exec], using a squirrelly builder.
func (db *Db) Exec(query Sqlizer) (sql.Result, error) {
	return Exec(db.DB, query)
}

func (tx *Tx) Exec(query Sqlizer) (sql.Result, error) {
	return Exec(tx.Tx, query)
}

// Query runs [database/sql.DB.Query] using a squirrelly builder.
func Query(db Querier, query Sqlizer) (*sql.Rows, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	return db.Query(sql, args...)
}

// Query runs [database/sql.DB.Query] using a squirrelly builder.
func (db *Db) Query(query Sqlizer) (*sql.Rows, error) {
	return Query(db.DB, query)
}

func (tx *Tx) Query(query Sqlizer) (*sql.Rows, error) {
	return Query(tx.Tx, query)
}

// QueryRow runs [database/sql.DB.QueryRow] using a squirrelly builder.
func QueryRow(db Querier, query Sqlizer) *sql.Row {
	sql, args, err := query.ToSql()
	if err != nil {
		panic(err)
	}

	return db.QueryRow(sql, args...)
}

// QueryRow runs [database/sql.DB.QueryRow] using a squirrelly builder.
func (db *Db) QueryRow(query Sqlizer) *sql.Row {
	return QueryRow(db.DB, query)
}

func (tx *Tx) QueryRow(query Sqlizer) *sql.Row {
	return QueryRow(tx.Tx, query)
}

// Get runs a query using a squirrelly builder (that should return one and only one result), and marshals the result into the data interface.
//
// The data argument must be a pointer, it supports any value that [database/sql.Rows.Scan] supports, or structs that are tagged using the `sq` tag, similar to how the [encoding/json.Marshal] function works using the `json` tag.
func (db *Db) Get(query Sqlizer, data interface{}) error {
	return DbGet(db, query, data)
}

func (tx *Tx) Get(query Sqlizer, data any) error {
	return DbGet(tx, query, data)
}

func DbGet(db DbLike, query Sqlizer, data any) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}

	return structScan(rows, data)
}

// GetAll runs a query using a squirrelly builder, and marshals the resulting records into the data interface.
//
// The container argument must be a pointer to a slice, the slice may be of any value that [database/sql.Rows.Scan] supports, or structs that are tagged using the `sq` tag, similar to how the [encoding/json.Marshal] function works using the `json` tag.
func (db *Db) GetAll(query Sqlizer, container interface{}) error {
	return DbGetAll(db, query, container)
}

func (tx *Tx) GetAll(query Sqlizer, container any) error {
	return DbGetAll(tx, query, container)
}

func DbGetAll(db DbLike, query Sqlizer, container any) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// we don't need to error check -- rows can't be closed yet
	columns, _ := rows.Columns()

	containerValue := reflect.ValueOf(container)
	if containerValue.Kind() != reflect.Ptr {
		return errors.New("Container is not a pointer")
	}

	containerValue = containerValue.Elem()
	if containerValue.Kind() != reflect.Slice {
		return errors.New("Container is not a pointer to a slice")
	}

	containerType := containerValue.Type()
	elemType := containerType.Elem()

	// determine if element is concrete type, or pointer type
	// because we need to pass the right type into append
	isPtr := elemType.Kind() == reflect.Ptr

	// if it's a pointer type get the concrete type
	if isPtr {
		elemType = elemType.Elem()
	}

	mapper := getMapper()
	fieldTraversals := mapper.TraversalsByName(elemType, columns)
	for idx, f := range fieldTraversals {
		if len(f) == 0 {
			return fmt.Errorf("missing destination name %s in %s", columns[idx], elemType.Name())
		}
	}

	rawValues := []reflect.Value{}
	err = scanRows(rows, func(row *sql.Rows) error {
		elem := reflect.New(elemType)

		var elemValues []interface{}
		if len(columns) == 1 {
			elemValues = []interface{}{elem.Interface()}
		} else {
			elemValues = make([]interface{}, len(columns))
			for idx, traversal := range fieldTraversals {
				field := reflectx.FieldByIndexes(elem, traversal)
				elemValues[idx] = field.Addr().Interface()
			}
		}

		err := row.Scan(elemValues...)
		if err != nil {
			return err
		}

		if !isPtr {
			elem = reflect.Indirect(elem)
		}

		rawValues = append(rawValues, elem)
		return nil
	})
	if err != nil {
		return err
	}

	containerValue.Set(reflect.MakeSlice(containerType, len(rawValues), len(rawValues)))
	for idx, value := range rawValues {
		containerValue.Index(idx).Set(value)
	}

	return nil
}

func scanRows(rows *sql.Rows, fn func(*sql.Rows) error) error {
	defer rows.Close()
	for rows.Next() {
		err := rows.Err()
		if err != nil {
			return err
		}

		err = fn(rows)
		if err != nil {
			return err
		}
	}

	return nil
}

func structScan(rows *sql.Rows, destination interface{}) error {
	dest := reflect.ValueOf(destination)
	if dest.Kind() != reflect.Ptr {
		return errors.New("destination is not a pointer")
	}
	typ := dest.Elem().Type()
	if typ.Kind() != reflect.Struct {
		return errors.New("destination is not a struct")
	}

	defer rows.Close()
	if !rows.Next() {
		return sql.ErrNoRows
	}

	// value := reflect.New(typ)
	columns, _ := rows.Columns()

	mapper := getMapper()
	fieldTraversals := mapper.TraversalsByName(typ, columns)

	scanInterfaces := make([]interface{}, len(columns))
	for idx, f := range fieldTraversals {
		if len(f) == 0 {
			return fmt.Errorf("missing destination name %s in %s", columns[idx], typ.Name())
		}

		field := reflectx.FieldByIndexes(dest, f)
		scanInterfaces[idx] = field.Addr().Interface()
	}

	err := rows.Scan(scanInterfaces...)
	if err != nil {
		return err
	}

	if rows.Next() {
		return errors.New("trying to scan multiple rows into a single struct")
	}

	return nil
}
