package memdb

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/rosedblabs/wal"
)

// BTree is a memory based btree implementation of the Index interface
// It is a wrapper around the google/btree package: github.com/google/btree
type BTree struct {
	lock *sync.RWMutex
	tree *btree.BTreeG[*btreeItem]
	less func(a, b *btreeItem) bool
}

type btreeItem struct {
	key []byte
	pos *wal.ChunkPosition
}

func newBTree(lessFunc func(a, b []byte) bool) *BTree {
	less := func(a, b *btreeItem) bool {
		if a == nil {
			return true
		}
		if b == nil {
			return false
		}
		if lessFunc == nil {
			return bytes.Compare(a.key, b.key) < 0
		}
		return lessFunc(a.key, b.key)
	}
	return &BTree{
		lock: new(sync.RWMutex),
		tree: btree.NewG(32, less),
		less: less,
	}
}

// Put key and position into the index.
func (mt *BTree) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	oldValue, _ := mt.tree.ReplaceOrInsert(&btreeItem{key: key, pos: position})
	if oldValue != nil {
		return oldValue.pos
	}
	return nil
}

// Get the position of the key in the index.
func (mt *BTree) Get(key []byte) *wal.ChunkPosition {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	value, _ := mt.tree.Get(&btreeItem{key: key})
	if value != nil {
		return value.pos
	}
	return nil
}

// Delete the index of the key.
func (mt *BTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	value, _ := mt.tree.Delete(&btreeItem{key: key})
	if value != nil {
		return value.pos, true
	}
	return nil, false
}

// Size represents the number of keys in the index.
func (mt *BTree) Size() int {
	return mt.tree.Len()
}

// Ascend iterates over items in ascending order and invokes the handler function for each item.
// If the handler function returns false, iteration stops.
func (mt *BTree) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.Ascend(func(i *btreeItem) bool {
		cont, err := handleFn(i.key, i.pos)
		if err != nil {
			return false
		}
		return cont
	})
}

// Descend iterates over items in descending order and invokes the handler function for each item.
// If the handler function returns false, iteration stops.
func (mt *BTree) Descend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.Descend(func(i *btreeItem) bool {
		cont, err := handleFn(i.key, i.pos)
		if err != nil {
			return false
		}
		return cont
	})
}

// AscendRange iterates in ascending order within [startKey, endKey], invoking handleFn.
// Stops if handleFn returns false.
func (mt *BTree) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.AscendRange(&btreeItem{key: startKey}, &btreeItem{key: endKey}, func(i *btreeItem) bool {
		cont, err := handleFn(i.key, i.pos)
		if err != nil {
			return false
		}
		return cont
	})
}

// DescendRange iterates in descending order within [startKey, endKey], invoking handleFn.
// Stops if handleFn returns false.
func (mt *BTree) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.DescendRange(&btreeItem{key: startKey}, &btreeItem{key: endKey}, func(i *btreeItem) bool {
		cont, err := handleFn(i.key, i.pos)
		if err != nil {
			return false
		}
		return cont
	})
}

// AscendGreaterOrEqual iterates in ascending order, starting from key >= given key,
// invoking handleFn. Stops if handleFn returns false.
func (mt *BTree) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.AscendGreaterOrEqual(&btreeItem{key: key}, func(i *btreeItem) bool {
		cont, err := handleFn(i.key, i.pos)
		if err != nil {
			return false
		}
		return cont
	})
}

// DescendLessOrEqual iterates in descending order, starting from key <= given key,
// invoking handleFn. Stops if handleFn returns false.
func (mt *BTree) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.DescendLessOrEqual(&btreeItem{key: key}, func(i *btreeItem) bool {
		cont, err := handleFn(i.key, i.pos)
		if err != nil {
			return false
		}
		return cont
	})
}

// Iterator returns an index iterator.
func (mt *BTree) Iterator(reverse bool) *BTreeIterator {
	if mt.tree == nil {
		return nil
	}
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	return newBTreeIterator(mt.tree, reverse, mt.less)
}

// BTreeIterator represents a B-tree index iterator
type BTreeIterator struct {
	tree    *btree.BTreeG[*btreeItem] // underlying B-tree implementation
	reverse bool                      // indicates whether to traverse in descending order
	current *btreeItem                // current element being traversed
	valid   bool                      // indicates if the iterator is valid
	less    func(a, b *btreeItem) bool
}

func newBTreeIterator(tree *btree.BTreeG[*btreeItem], reverse bool, less func(a, b *btreeItem) bool) *BTreeIterator {
	var current *btreeItem
	var valid bool
	if tree.Len() > 0 {
		if reverse {
			current, _ = tree.Max()
		} else {
			current, _ = tree.Min()
		}
		valid = true
	}

	return &BTreeIterator{
		tree:    tree.Clone(),
		reverse: reverse,
		current: current,
		valid:   valid,
		less:    less,
	}
}

// Rewind resets the iterator to its initial position.
func (it *BTreeIterator) Rewind() {
	if it.tree == nil || it.tree.Len() == 0 {
		return
	}

	if it.reverse {
		it.current, _ = it.tree.Max()
	} else {
		it.current, _ = it.tree.Min()
	}
	it.valid = true
}

// Seek positions the cursor to the element with the specified key.
func (it *BTreeIterator) Seek(key []byte) {
	if it.tree == nil || !it.valid {
		return
	}

	seekItem := &btreeItem{key: key}
	it.valid = false
	if it.reverse {
		it.tree.DescendLessOrEqual(seekItem, func(i *btreeItem) bool {
			it.current = i
			it.valid = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(seekItem, func(i *btreeItem) bool {
			it.current = i
			it.valid = true
			return false
		})
	}
}

// Next moves the cursor to the next element.
func (it *BTreeIterator) Next() {
	if it.tree == nil || !it.valid {
		return
	}

	it.valid = false
	if it.reverse {
		it.tree.DescendLessOrEqual(it.current, func(i *btreeItem) bool {
			if !it.less(i, it.current) {
				return true
			}
			it.current = i
			it.valid = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(it.current, func(i *btreeItem) bool {
			if !it.less(it.current, i) {
				return true
			}
			it.current = i
			it.valid = true
			return false
		})
	}

	if !it.valid {
		it.current = nil
	}
}

// Valid checks if the iterator is still valid for reading.
func (it *BTreeIterator) Valid() bool {
	return it.valid
}

// Key returns the key of the current element.
func (it *BTreeIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.current.key
}

// Value returns the value (chunk position) of the current element.
func (it *BTreeIterator) Value() *wal.ChunkPosition {
	if !it.valid {
		return nil
	}
	return it.current.pos
}

// Close releases the resources associated with the iterator.
func (it *BTreeIterator) Close() {
	it.tree.Clear(true)
	it.tree = nil
	it.current = nil
	it.valid = false
}
