> 本项目修改 [**rosedblabs/rosedb**](https://github.com/rosedblabs/rosedb)，并包含 NOTICE 文件中列出的修改内容。

## MemDB 是什么？
EimDB 是英文 **Embedded in-memory database** 的简写，是一个基于 [Bitcask](https://riak.com/assets/bitcask-intro.pdf) 存储模型，轻量、快速、可靠的 KV 存储引擎。

Bitcask 存储模型的设计主要受到日志结构化的文件系统和日志文件合并的启发。

## 设计概述

![](https://github.com/hupeh/memdb/blob/main/docs/imgs/design-overview-memdb.png)

MemDB 存储数据的文件使用预写日志（Write Ahead Log），这些日志文件是具有分块结构的只追加写入（append-only）文件。

> wal: https://github.com/rosedblabs/wal

## 主要特点
### 优势

<details>
  <summary><b>读写低延迟</b></summary>
  这是由于 Bitcask 存储模型文件的追加写入特性，充分利用顺序 IO 的优势。
</details>

<details>
  <summary><b>高吞吐量，即使数据完全无序</b></summary>
  写入 MemDB 的数据不需要在磁盘上排序，Bitcask 的日志结构文件设计在写入过程中减少了磁盘磁头的移动。
</details>

<details>
  <summary><b>能够处理大于内存的数据集，性能稳定</b></summary>
  MemDB 的数据访问涉及对内存中的索引数据结构进行直接查找，这使得即使数据集非常大，查找数据也非常高效。
</details>

<details>
  <summary><b>一次磁盘 IO 可以获取任意键值对</b></summary>
  MemDB 的内存索引数据结构直接指向数据所在的磁盘位置，不需要多次磁盘寻址来读取一个值，有时甚至不需要寻址，这归功于操作系统的文件系统缓存以及 WAL 的 block 缓存。
</details>

<details>
  <summary><b>性能快速稳定</b></summary>
  MemDB 写入操作最多需要一次对当前打开文件的尾部的寻址，然后进行追加写入，写入后会更新内存。这个流程不会受到数据库数据量大小的影响，因此性能稳定。
</details>

<details>
  <summary><b>崩溃恢复快速</b></summary>
  使用 MemDB 的崩溃恢复很容易也很快，因为 MemDB 文件是只追加写入一次的。恢复操作需要检查记录并验证CRC数据，以确保数据一致。
</details>

<details>
  <summary><b>备份简单</b></summary>
  在大多数系统中，备份可能非常复杂。MemDB 通过其只追加写入一次的磁盘格式简化了此过程。任何按磁盘块顺序存档或复制文件的工具都将正确备份或复制 MemDB 数据库。
</details>

<details>
  <summary><b>批处理操作可以保证原子性、一致性和持久性</b></summary>
  MemDB 支持批处理操作，这些操作是原子、一致和持久的。批处理中的新写入操作在提交之前被缓存在内存中。如果批处理成功提交，批处理中的所有写入操作将持久保存到磁盘。如果批处理失败，批处理中的所有写入操作将被丢弃。
  即一个批处理操作中的所有写入操作要么全部成功，要么全部失败。
</details>

<details>
  <summary><b>支持可以反向和正向迭代的迭代器</b></summary>
  MemDB 支持正向和反向迭代器，这些迭代器可以在数据库中的任何位置开始迭代。迭代器可以用于扫描数据库中的所有键值对，也可以用于扫描数据库中的某个范围的键值对，迭代器从索引中获取位置信息，然后直接从磁盘中读取数据，因此迭代器的性能非常高。
</details>

<details>
  <summary><b>支持 Watch 功能</b></summary>
  MemDB 支持 Watch 功能，DB 中的 key 发生变化时你可以得到一个事件通知。
</details>

<details>
  <summary><b>支持 Key 的过期时间</b></summary>
  MemDB 支持为 key 设置过期时间，过期后 key 将被自动删除。
</details>

<details>
    <summary><b>支持自定义排序</b></summary>
    MemDB 支持运行时阶段自定义排序，不影响存储。
</details>

### 缺点

<details>
    <summary><b>所有的 key 必须在内存中维护</b></summary>
    MemDB 始终将所有 key 保留在内存中，这意味着您的系统必须具有足够的内存来容纳所有的 key。
</details>

## 快速上手

### 基本操作

```go
package main

import "github.com/hupeh/memdb"

func main() {
	// 指定选项
	options := memdb.DefaultOptions
	options.DirPath = "/tmp/memdb_basic"

	// 打开数据库
	db, err := memdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// 设置键值对
	err = db.Put([]byte("name"), []byte("memdb"))
	if err != nil {
		panic(err)
	}

	// 获取键值对
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// 删除键值对
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```

### 批处理操作
```go
	// 创建批处理
	batch := db.NewBatch(memdb.DefaultBatchOptions)

	// 设置键值对
	_ = batch.Put([]byte("name"), []byte("memdb"))

	// 获取键值对
	val, _ := batch.Get([]byte("name"))
	println(string(val))

	// 删除键值对
	_ = batch.Delete([]byte("name"))

	// 提交批处理
	_ = batch.Commit()
```
完整代码可查看 [examples 示例代码](https://github.com/hupeh/memdb/tree/main/examples)。
