package internal

import (
	"encoding/csv"
	"log"
	"os"

	constants "github.com/worker-pool/constant"
)

func OpenCsvFile() (*csv.Reader, *os.File, error) {
    log.Println("=> open csv file")

    f, err := os.Open(constants.CsvFile)
    if err != nil {
        return nil, nil, err
    }

    reader := csv.NewReader(f)
    return reader, f, nil
}