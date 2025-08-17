package main

import (
	"runtime"

	"github.com/hupeh/memdb"
)

// this file shows how to use the batch operations of memdb

func main() {
	// specify the options
	options := memdb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\memdb_batch"
	} else {
		options.DirPath = "/tmp/memdb_batch"
	}

	// open a database
	db, err := memdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// create a batch
	batch := db.NewBatch(memdb.DefaultBatchOptions)

	// set a key
	_ = batch.Put([]byte("name"), []byte("memdb"))

	// get a key
	val, _ := batch.Get([]byte("name"))
	println(string(val))

	// delete a key
	_ = batch.Delete([]byte("name"))

	// commit the batch
	_ = batch.Commit()

	// if you want to cancel batch, you must call rollback().
	// _= batch.Rollback()

	// once a batch is committed, it can't be used again
	// _ = batch.Put([]byte("name1"), []byte("memdb1")) // don't do this!!!
}
