package main

import (
	"log"
	"runtime"
	"time"

	"github.com/hupeh/memdb"
)

// this file shows how to use the Expiry/TTL feature of memdb.
func main() {
	// specify the options
	options := memdb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\memdb_ttl"
	} else {
		options.DirPath = "/tmp/memdb_ttl"
	}

	// open a database
	db, err := memdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// when you put a key-value pair, you can specify the ttl.
	err = db.PutWithTTL([]byte("name"), []byte("memdb"), time.Second*5)
	if err != nil {
		panic(err)
	}
	// now you can get the ttl of the key.
	ttl, err := db.TTL([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(ttl.String())

	_ = db.Put([]byte("name2"), []byte("memdb"))
	//and you can also set the ttl of the key after you put it.
	err = db.Expire([]byte("name2"), time.Second*2)
	if err != nil {
		panic(err)
	}
	ttl, err = db.TTL([]byte("name2"))
	if err != nil {
		log.Println(err)
	}
	println(ttl.String())
}
