package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/hupeh/memdb"
	"github.com/hupeh/memdb/utils"
)

// this file shows how to use the Watch feature of memdb.

func main() {
	// specify the options
	options := memdb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\memdb_watch"
	} else {
		options.DirPath = "/tmp/memdb_watch"
	}
	options.WatchQueueSize = 1000

	// open a database
	db, err := memdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// run a new goroutine to handle db event.
	go func() {
		eventCh, err := db.Watch()
		if err != nil {
			return
		}
		for {
			event := <-eventCh
			// when db closed, the event will receive nil.
			if event == nil {
				fmt.Println("The db is closed, so the watch channel is closed.")
				return
			}
			// events can be captured here for processing
			fmt.Printf("Get a new event: key%s \n", event.Key)
		}
	}()

	// write some data
	for i := 0; i < 10; i++ {
		_ = db.Put(utils.GetTestKey(i), utils.RandomValue(64))
	}
	// delete some data
	for i := 0; i < 10/2; i++ {
		_ = db.Delete(utils.GetTestKey(i))
	}

	// wait for watch goroutine to finish.
	time.Sleep(1 * time.Second)
}
