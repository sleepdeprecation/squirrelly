package squirrelly

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/jmoiron/sqlx"
)

type Db struct {
	*sqlx.DB
}

func Open(driver, source string) (*Db, error) {
	sqldb, err := sqlx.Open(driver, source)
	if err != nil {
		return nil, err
	}

	return &Db{sqldb}, nil
}

func (db *Db) Exec(query Sqlizer) (sql.Result, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	return db.DB.Exec(sql, args...)
}

func (db *Db) Query(query Sqlizer) (*sqlx.Rows, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	return db.Queryx(sql, args...)
}

func (db *Db) QueryRow(query Sqlizer) *sqlx.Row {
	sql, args, err := query.ToSql()
	if err != nil {
		panic(err)
	}

	return db.QueryRowx(sql, args...)
}

func (db *Db) Get(query Sqlizer, data interface{}) error {
	row := db.QueryRow(query)
	err := row.Err()
	if err != nil {
		return err
	}

	return row.StructScan(data)
}

func (db *Db) GetAll(query Sqlizer, container interface{}) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}

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

	isStruct := elemType.Kind() == reflect.Struct

	rawValues := []reflect.Value{}
	err = scanRows(rows, func(row *sqlx.Rows) error {
		elem := reflect.New(elemType)

		var err error
		if isStruct {
			err = row.StructScan(elem.Interface())
		} else {
			err = row.Scan(elem.Interface())
		}

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

func scanRows(rows *sqlx.Rows, fn func(*sqlx.Rows) error) error {
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
