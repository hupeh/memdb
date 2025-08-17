package benchmark

import (
	"errors"
	"math/rand"
	"os"
	"runtime"
	"testing"

	"github.com/hupeh/memdb"
	"github.com/hupeh/memdb/utils"
	"github.com/stretchr/testify/assert"
)

var db *memdb.DB

func openDB() func() {
	options := memdb.DefaultOptions
	sysType := runtime.GOOS
	if sysType == "windows" {
		options.DirPath = "C:\\memdb_bench_test"
	} else {
		options.DirPath = "/tmp/memdb_bench_test"
	}

	var err error
	db, err = memdb.Open(options)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPutGet(b *testing.B) {
	closer := openDB()
	defer closer()

	b.Run("put", benchmarkPut)
	b.Run("get", benchmarkGet)
}

func BenchmarkBatchPutGet(b *testing.B) {
	closer := openDB()
	defer closer()

	b.Run("batchPut", benchmarkBatchPut)
	b.Run("batchGet", benchmarkBatchGet)
}

func benchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func benchmarkBatchPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	batch := db.NewBatch(memdb.DefaultBatchOptions)
	defer func() {
		_ = batch.Commit()
	}()
	for i := 0; i < b.N; i++ {
		err := batch.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func benchmarkBatchGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	batch := db.NewBatch(memdb.DefaultBatchOptions)
	defer func() {
		_ = batch.Commit()
	}()
	for i := 0; i < b.N; i++ {
		_, err := batch.Get(utils.GetTestKey(rand.Int()))
		if err != nil && !errors.Is(err, memdb.ErrKeyNotFound) {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && !errors.Is(err, memdb.ErrKeyNotFound) {
			b.Fatal(err)
		}
	}
}
