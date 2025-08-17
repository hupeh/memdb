package memdb

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Options specifies the options for opening a database.
type Options struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	Sync bool

	// BytesPerSync specifies the number of bytes to write before calling fsync.
	BytesPerSync uint32

	// WatchQueueSize the cache length of the watch queue.
	// if the size greater than 0, which means enable the watch.
	WatchQueueSize uint64

	// AutoMergeEnable enable the auto merge.
	// auto merge will be triggered when cron expr is satisfied.
	// cron expression follows the standard cron expression.
	// e.g. "0 0 * * *" means merge at 00:00:00 every day.
	// it also supports seconds optionally.
	// when enable the second field, the cron expression will be like this: "0/10 * * * * *" (every 10 seconds).
	// when auto merge is enabled, the db will be closed and reopened after merge done.
	// do not set this shecule too frequently, it will affect the performance.
	// refer to https://en.wikipedia.org/wiki/Cron
	AutoMergeCronExpr string

	// LessFunc is used for custom index sorting
	LessFunc func(key1, key2 []byte) bool
}

// BatchOptions specifies the options for creating a batch.
type BatchOptions struct {
	// Sync has the same semantics as Options.Sync.
	Sync bool
	// ReadOnly specifies whether the batch is read only.
	ReadOnly bool
}

// IteratorOptions defines configuration options for creating a new iterator.
type IteratorOptions struct {
	// Prefix specifies a key prefix for filtering. If set, the iterator will only
	// traverse keys that start with this prefix. Default is empty (no filtering).
	Prefix []byte

	// Reverse determines the traversal order. If true, the iterator will traverse
	// in descending order. Default is false (ascending order).
	Reverse bool

	// ContinueOnError determines how the iterator handles errors during iteration.
	// If true, the iterator will log errors and continue to the next entry.
	// If false, the iterator will stop and become invalid when an error occurs.
	ContinueOnError bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:           tempDBDir(),
	SegmentSize:       1 * GB,
	Sync:              false,
	BytesPerSync:      0,
	WatchQueueSize:    0,
	AutoMergeCronExpr: "",
	LessFunc:          nil,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:          nil,
	Reverse:         false,
	ContinueOnError: false,
}

var nameRand = rand.NewSource(time.Now().UnixNano())

func tempDBDir() string {
	return filepath.Join(os.TempDir(), "memdb-temp"+strconv.Itoa(int(nameRand.Int63())))
}
