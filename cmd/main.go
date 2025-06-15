package main

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/worker-pool/db"
	"github.com/worker-pool/internal"
)

func main() {
	start := time.Now()

	db, err := db.OpenDbConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	csvReader, csvFile, err := internal.OpenCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go internal.DispatchWorkers(db, jobs, wg)
	internal.ReadCsvPerLineThenSendPerWorker(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}