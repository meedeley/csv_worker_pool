package constants

const DBConnString = "root:123@tcp(127.0.0.1:3306)/belajar_worker?parseTime=true"

const DBMaxIdleConns = 100

const DBMaxConns = 200

const TotalWorker = 100

const CsvFile = "static/majestic_million.csv"

var DataHeaders = make([]string, 0)