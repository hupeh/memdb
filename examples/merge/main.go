package main

import (
	"runtime"

	"github.com/hupeh/memdb"
	"github.com/hupeh/memdb/utils"
)

// this file shows how to use the Merge feature of memdb.
// Merge is used to merge the data files in the database.
// It is recommended to use it when the database is not busy.

func main() {
	// specify the options
	options := memdb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\memdb_merge"
	} else {
		options.DirPath = "/tmp/memdb_merge"
	}

	// open a database
	db, err := memdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// write some data
	for i := 0; i < 100000; i++ {
		_ = db.Put([]byte(utils.GetTestKey(i)), utils.RandomValue(128))
	}
	// delete some data
	for i := 0; i < 100000/2; i++ {
		_ = db.Delete([]byte(utils.GetTestKey(i)))
	}

	// then merge the data files
	// all the invalid data will be removed, and the valid data will be merged into the new data files.
	_ = db.Merge(true)
}
