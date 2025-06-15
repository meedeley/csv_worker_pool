package internal

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	constants "github.com/worker-pool/constant"
)

func DispatchWorkers(db *sql.DB, jobs <-chan []any, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= constants.TotalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			batchSize := 1500
			rowsToInsert := make([][]interface{}, 0, batchSize)
			counter := 0

			batchTicker := time.NewTicker(5 * time.Second)
			defer batchTicker.Stop()

			for {
				select {
				case job, ok := <-jobs:
					if !ok {
						if len(rowsToInsert) > 0 {
							doTheJob(workerIndex, &counter, db, rowsToInsert)
							rowsToInsert = rowsToInsert[:0] 
						}
						return
					}

					rowsToInsert = append(rowsToInsert, job)
					wg.Done()

					if len(rowsToInsert) >= batchSize {
						doTheJob(workerIndex, &counter, db, rowsToInsert)
						rowsToInsert = rowsToInsert[:0]
					}
				case <-batchTicker.C:
					if len(rowsToInsert) > 0 {
						doTheJob(workerIndex, &counter, db, rowsToInsert)
						rowsToInsert = rowsToInsert[:0] // Reset slice
					}
				}
			}
		}(workerIndex, db, jobs, wg)
	}
}

func ReadCsvPerLineThenSendPerWorker(csvReader *csv.Reader, jobs chan<- []any, wg *sync.WaitGroup) {
	for {
		row, err := csvReader.Read()

		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(constants.DataHeaders) == 0 {
			constants.DataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, 0, len(row)) 
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered
	}

	close(jobs)
}

func doTheJob(workerIndex int, counter *int, db *sql.DB, values [][]interface{}) {
	if len(values) == 0 {
		return
	}

	placeholders := make([]string, len(values))
	flatValues := make([]interface{}, 0, len(values)*len(constants.DataHeaders)) 
	for i, row := range values {
		placeholders[i] = fmt.Sprintf("(%s)", strings.Join(generateQuestionsMark(len(constants.DataHeaders)), ","))
		flatValues = append(flatValues, row...) 
	}

	query := fmt.Sprintf("INSERT INTO domain (%s) VALUES %s",
		strings.Join(constants.DataHeaders, ","),
		strings.Join(placeholders, ","),
	)

	var outerError error
	func(outerError *error) {
		defer func() {
			if r := recover(); r != nil {
				*outerError = fmt.Errorf("panic in doTheJob: %v", r)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := db.ExecContext(ctx, query, flatValues...)
		if err != nil {
			log.Printf("Error worker %d inserting %d data: %v", workerIndex, len(values), err)
			*outerError = err
			return
		}
	}(&outerError)

	if outerError != nil {
		log.Printf("Worker %d failed to insert batch of %d data due to: %v", workerIndex, len(values), outerError)
	} else {
		*counter += len(values)
		if *counter%1000 == 0 { // Ubah threshold log agar sesuai dengan batch size
			log.Println("=> worker", workerIndex, "inserted", *counter, "data")
		}
	}
}

func generateQuestionsMark(n int) []string {
	s := make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = "?"
	}
	return s
}