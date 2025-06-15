package db

import (
	"database/sql"
	"log"

	constants "github.com/worker-pool/constant"
	_ "github.com/go-sql-driver/mysql"
)

func OpenDbConnection() (*sql.DB, error) {
	log.Println("=> open db connection")

	db, err := sql.Open("mysql", constants.DBConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(constants.DBMaxConns)
	db.SetMaxIdleConns(constants.DBMaxIdleConns)

	return db, nil
}
