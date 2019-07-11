package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type callEventStore interface {
	Prepare() error
	Create(CallEventFile) error
}

type MySqlCallEventStore struct {
	user     string
	password string
	host     string
	port     string
	database string
	table    string
}

func NewMySqlEventStore(user, password, host, port, database, table string) *MySqlCallEventStore {
	return &MySqlCallEventStore{user, password, host, port, database, table}
}
func (s *MySqlCallEventStore) Prepare() error {
	connection, err := s.getConnection()
	if err != nil {
		return err
	}
	defer connection.Close()

	tableExists, err := s.tableExists(connection)
	if err != nil {
		return err
	}

	if tableExists > 0 {
		return nil
	}

	return s.createTable(connection)
}

func (s *MySqlCallEventStore) createTable(connection *sql.DB) error {
	tableSchema := `
		CREATE TABLE %s ( 
			id INT NOT NULL AUTO_INCREMENT, 
			event_date_time DATETIME NOT NULL,
			event_action VARCHAR(20) NOT NULL,
			call_ref VARCHAR(20) NOT NULL,
			event_value DECIMAL(10,2) NULL,
			event_currency_code VARCHAR(3) NULL,
			PRIMARY KEY (id) 
		)
	`
	query := fmt.Sprintf(tableSchema, s.table)
	_, err := connection.Exec(query)

	return err
}

func (s *MySqlCallEventStore) getConnection() (*sql.DB, error) {
	return sql.Open("mysql", s.user+":"+s.password+"@/"+s.database+"")
}

func (s *MySqlCallEventStore) tableExists(connection *sql.DB) (int, error) {
	tableExistsQuery := fmt.Sprintf("SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = %#v) AND (TABLE_NAME = %#v)",
		s.database,
		s.table,
	)

	rows, err := connection.Query(tableExistsQuery)
	if err != nil {
		return 0, err
	}

	var tableExists int
	var queryErr error
	for rows.Next() {
		queryErr = rows.Scan(&tableExists)
	}

	return tableExists, queryErr
}

func (s *MySqlCallEventStore) Create(file CallEventFile) error {
	dataRows := file.ValidData()
	if len(dataRows) == 0 {
		return nil
	}

	connection, err := s.getConnection()
	if err != nil {
		return err
	}

	insertTemplate := "INSERT INTO %s(event_date_time, event_action, call_ref, event_value, event_currency_code) VALUES "
	query := fmt.Sprintf(insertTemplate, s.table)
	vals := []interface{}{}

	lastRowIndex := len(dataRows) - 1

	for i, row := range dataRows {
		if i == lastRowIndex {
			query += "(?, ?, ?, ?, ?)"
		} else {
			query += "(?, ?, ?, ?, ?),"
		}
		vals = append(vals, row[COL_EVENT_DATETIME], row[COL_EVENT_ACTION], row[COL_CALL_REF], row[COL_EVENT_VAL], row[COL_EVENT_CURRENCY_CODE])
	}

	stmt, err := connection.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(vals...)

	return err
}
