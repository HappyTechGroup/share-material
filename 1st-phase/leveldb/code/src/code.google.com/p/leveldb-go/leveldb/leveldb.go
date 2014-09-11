// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package leveldb provides an ordered key/value store.
//
// BUG: This package is incomplete.
package leveldb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memdb"
	"code.google.com/p/leveldb-go/leveldb/record"
	"code.google.com/p/leveldb-go/leveldb/table"
)

const (
	// l0CompactionTrigger is the number of files at which level-0 compaction
	// starts.
	l0CompactionTrigger = 4

	// l0SlowdownWritesTrigger is the soft limit on number of level-0 files.
	// We slow down writes at this point.
	l0SlowdownWritesTrigger = 8

	// l0StopWritesTrigger is the maximum number of level-0 files. We stop
	// writes at this point.
	l0StopWritesTrigger = 12

	// minTableCacheSize is the minimum size of the table cache.
	minTableCacheSize = 64

	// numNonTableCacheFiles is an approximation for the number of MaxOpenFiles
	// that we don't use for table caches.
	numNonTableCacheFiles = 10
)

// TODO: document DB.
type DB struct {
	dirname string // 数据存储目录
	opts    *db.Options
	icmp    internalKeyComparer // 内部键比较函数
	// icmpOpts is a copy of opts that overrides the Comparer to be icmp.
	icmpOpts db.Options

	tableCache tableCache

	// TODO: describe exactly what this mutex protects. So far: every field
	// below.
	mu sync.Mutex

	fileLock  io.Closer
	logNumber uint64
	logFile   db.File
	log       *record.Writer

	versions versionSet

	// mem is non-nil and the MemDB pointed to is mutable. imm is possibly
	// nil, but if non-nil, the MemDB pointed to is immutable and will be
	// copied out as an on-disk table. mem's sequence numbers are all
	// higher than imm's, and imm's sequence numbers are all higher than
	// those on-disk.

	// sequence number的作用是什么？

	mem, imm *memdb.MemDB

	compactionCond sync.Cond
	compacting     bool

	closed bool

	pendingOutputs map[uint64]struct{}
}

// 这句代码的作用？
var _ db.DB = (*DB)(nil)

func (d *DB) Get(key []byte, opts *db.ReadOptions) ([]byte, error) {
	d.mu.Lock()
	// TODO: add an opts.LastSequence field, or a DB.Snapshot method?
	snapshot := d.versions.lastSequence
	current := d.versions.currentVersion()
	// TODO: do we need to ref-count the current version, so that we don't
	// delete its underlying files if we have a concurrent compaction?

	memtables := [2]*memdb.MemDB{d.mem, d.imm}

	d.mu.Unlock()

	// 将用户提供的key转换成key的内部表示形式
	// 内部key的形式为：先是ukey，然后是internalKeyKindMax，然后是snapshot
	ikey := makeInternalKey(nil, key, internalKeyKindMax, snapshot)

	// Look in the memtables before going to the on-disk current version.
	// 先从mem和imm中查找
	for _, mem := range memtables {
		if mem == nil {
			continue
		}
		// func internalGet(t db.Iterator, ucmp db.Comparer, ukey []byte) (value []byte, conclusive bool, err error)
		value, conclusive, err := internalGet(mem.Find(ikey, opts), d.icmp.userCmp, key)
		// conclusive: 决定性的、确凿的
		if conclusive {
			return value, err
		}
	}

	// TODO: update stats, maybe schedule compaction.
	// 在mem和imm中尚未找到
	// 那么就从最新version在各个level上持有的文件列表中查找
	return current.get(ikey, &d.tableCache, d.icmp.userCmp, opts)
}

func (d *DB) Set(key, value []byte, opts *db.WriteOptions) error {
	var batch Batch
	// 先将key、value编码到batch中
	batch.Set(key, value)
	// 然后将batch应用到内存memtable中
	return d.Apply(batch, opts)
}

func (d *DB) Delete(key []byte, opts *db.WriteOptions) error {
	var batch Batch
	batch.Delete(key)
	return d.Apply(batch, opts)
}

// 这个方法很关键
func (d *DB) Apply(batch Batch, opts *db.WriteOptions) error {
	if len(batch.data) == 0 {
		return nil
	}
	// batch.count获得batch中元素的个数
	n := batch.count()
	// invalidBatchCount = 1<<32 - 1
	if n == invalidBatchCount {
		return errors.New("leveldb: invalid batch")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// 这里的makeRoomForWrite还得仔细阅读理解一下
	if err := d.makeRoomForWrite(false); err != nil {
		return err
	}

	seqNum := d.versions.lastSequence + 1
	batch.setSeqNum(seqNum)
	d.versions.lastSequence += uint64(n)

	// 先写日志
	// Write the batch to the log.
	// TODO: drop and re-acquire d.mu around the I/O.
	w, err := d.log.Next()
	if err != nil {
		return fmt.Errorf("leveldb: could not create log entry: %v", err)
	}
	if _, err = w.Write(batch.data); err != nil {
		return fmt.Errorf("leveldb: could not write log entry: %v", err)
	}
	if opts.GetSync() {
		if err = d.log.Flush(); err != nil {
			return fmt.Errorf("leveldb: could not flush log entry: %v", err)
		}
		if err = d.logFile.Sync(); err != nil {
			return fmt.Errorf("leveldb: could not sync log entry: %v", err)
		}
	}

	// Apply the batch to the memtable.
	for iter, ikey := batch.iter(), internalKey(nil); ; seqNum++ {
		kind, ukey, value, ok := iter.next()
		if !ok {
			break
		}
		ikey = makeInternalKey(ikey, ukey, kind, seqNum)
		// 存入的是内部key
		d.mem.Set(ikey, value, nil)
	}

	if seqNum != d.versions.lastSequence+1 {
		panic("leveldb: inconsistent batch count")
	}
	return nil
}

func (d *DB) Find(key []byte, opts *db.ReadOptions) db.Iterator {
	panic("unimplemented")
}

func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	for d.compacting {
		d.compactionCond.Wait()
	}
	err := d.tableCache.Close()
	err = firstError(err, d.log.Close())
	err = firstError(err, d.logFile.Close())
	err = firstError(err, d.fileLock.Close())
	d.closed = true
	return err
}

type fileNumAndName struct {
	num  uint64
	name string
}

type fileNumAndNameSlice []fileNumAndName

func (p fileNumAndNameSlice) Len() int { return len(p) }
func (p fileNumAndNameSlice) Less(i, j int) bool { return p[i].num < p[j].num }
func (p fileNumAndNameSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// 创建leveldb存储目录时，需要传入db.Options, 那么db.Options有哪些属性呢？
func createDB(dirname string, opts *db.Options) (retErr error) {
	const manifestFileNum = 1
	// versionEdit保存版本之间的差异，oldVersion _+ versionEdit = newVersion
	ve := versionEdit{
		// 原始key比较器的名称
		comparatorName: opts.GetComparer().Name(),
		// ?
		nextFileNumber: manifestFileNum + 1,
	}
	// 先创建manifest文件，往里面写一些元信息，如上面versionEdit的comparatorName、nextFileNumber字段
	manifestFilename := dbFilename(dirname, fileTypeManifest, manifestFileNum)
	f, err := opts.GetFileSystem().Create(manifestFilename)
	if err != nil {
		return fmt.Errorf("leveldb: could not create %q: %v", manifestFilename, err)
	}
	defer func() {
		if retErr != nil {
			opts.GetFileSystem().Remove(manifestFilename)
		}
	}()
	defer f.Close()

	// 生成一个record.Writer对象
	recWriter := record.NewWriter(f)

	// 返回一个record.SingleWriter对象w
	w, err := recWriter.Next()
	if err != nil {
		return err
	}
	// 将ve中的数据写入manifest文件中
	err = ve.encode(w)
	if err != nil {
		return err
	}
	// 加入重启点, 压缩，crc校验？
	err = recWriter.Close()
	if err != nil {
		return err
	}
	// 创建current文件，其中写入了当前的manifest文件名
	return setCurrentFile(dirname, opts.GetFileSystem(), manifestFileNum)
}

// Open opens a LevelDB whose files live in the given directory.
func Open(dirname string, opts *db.Options) (*DB, error) {
	d := &DB{
		dirname:        dirname,
		opts:           opts,
		icmp:           internalKeyComparer{opts.GetComparer()},
		pendingOutputs: make(map[uint64]struct{}),
	}
	if opts != nil {
		d.icmpOpts = *opts
	}
	d.icmpOpts.Comparer = d.icmp
	tableCacheSize := opts.GetMaxOpenFiles() - numNonTableCacheFiles
	if tableCacheSize < minTableCacheSize {
		tableCacheSize = minTableCacheSize
	}
	// tableCache初始化
	d.tableCache.init(dirname, opts.GetFileSystem(), &d.icmpOpts, tableCacheSize)
	// 初始化一个MemDB
	d.mem = memdb.New(&d.icmpOpts)

	// sync.Cond在Locker的基础上增加的一个消息通知的功能。
	// Cond有三个方法：Wait，Signal，Broadcast。
	// Wait添加一个计数，也就是添加一个阻塞的goroutine。
	// Signal解除一个goroutine的阻塞，计数减一。
	// Broadcast接触所有wait goroutine的阻塞。
	d.compactionCond = sync.Cond{L: &d.mu}
	fs := opts.GetFileSystem()

	d.mu.Lock()
	defer d.mu.Unlock()

	// Lock the database directory.
	// If the directory already exists, MkdirAll does nothing and returns nil.
	// 如果目录已经存在，则MkdirAll啥都不干
	err := fs.MkdirAll(dirname, 0755)
	if err != nil {
		return nil, err
	}
	// 创建LOCK文件，并加文件锁
	fileLock, err := fs.Lock(dbFilename(dirname, fileTypeLock, 0))
	if err != nil {
		return nil, err
	}
	defer func() {
		if fileLock != nil {
			fileLock.Close()
		}
	}()

	// 若CURRENT文件不存在，则调用createDB
	if _, err := fs.Stat(dbFilename(dirname, fileTypeCurrent, 0)); os.IsNotExist(err) {
		// Create the DB if it did not already exist.
		if err := createDB(dirname, opts); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("leveldb: database %q: %v", dirname, err)
	} else if opts.GetErrorIfDBExists() {
		return nil, fmt.Errorf("leveldb: database %q already exists", dirname)
	}

	// Load the version set.
	// 先读取CURRENT文件内容，获取manifest文件名，然后逐条记录读取manifest文件的内容，根据内容生成一个新version，放入d.versions中
	err = d.versions.load(dirname, opts)
	if err != nil {
		return nil, err
	}

	// Replay any newer log files than the ones named in the manifest.
	var ve versionEdit
	ls, err := fs.List(dirname)
	if err != nil {
		return nil, err
	}
	var logFiles fileNumAndNameSlice
	for _, filename := range ls {
		ft, fn, ok := parseDBFilename(filename)
		if ok && ft == fileTypeLog && (fn >= d.versions.logNumber || fn == d.versions.prevLogNumber) {
			logFiles = append(logFiles, fileNumAndName{fn, filename})
		}
	}
	sort.Sort(logFiles)
	for _, lf := range logFiles {
		// 根据日志文件重做日志中记录的操作，先将这些操作记录存入一个临时的memtable中，然后转存入磁盘上level0存储文件中
		maxSeqNum, err := d.replayLogFile(&ve, fs, filepath.Join(dirname, lf.name))
		if err != nil {
			return nil, err
		}
		d.versions.markFileNumUsed(lf.num)
		// 设置最新的操作序列号
		if d.versions.lastSequence < maxSeqNum {
			d.versions.lastSequence = maxSeqNum
		}
	}

	// Create an empty .log file.
	// 创建一个新的空log文件
	ve.logNumber = d.versions.nextFileNum()
	d.logNumber = ve.logNumber
	logFile, err := fs.Create(dbFilename(dirname, fileTypeLog, ve.logNumber))
	if err != nil {
		return nil, err
	}
	defer func() {
		if logFile != nil {
			logFile.Close()
		}
	}()
	d.log = record.NewWriter(logFile)

	// Write a new manifest to disk.
	// 根据前面重做日志得到的ve的信息创建一个新的manifest文件
	// 并在CURRENT文件中指向这个新manifest文件
	if err := d.versions.logAndApply(dirname, &ve); err != nil {
		return nil, err
	}

	d.deleteObsoleteFiles()
	// 尝试调度compaction
	d.maybeScheduleCompaction()

	d.logFile, logFile = logFile, nil
	d.fileLock, fileLock = fileLock, nil
	return d, nil
}

// replayLogFile replays the edits in the named log file.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayLogFile(ve *versionEdit, fs db.FileSystem, filename string) (maxSeqNum uint64, err error) {
	file, err := fs.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		mem      *memdb.MemDB
		batchBuf = new(bytes.Buffer)
		ikey     = make(internalKey, 512)
		rr       = record.NewReader(file)
	)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		_, err = io.Copy(batchBuf, r)
		if err != nil {
			return 0, err
		}

		if batchBuf.Len() < batchHeaderLen {
			return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
		}
		b := Batch{batchBuf.Bytes()}
		seqNum := b.seqNum()
		seqNum1 := seqNum + uint64(b.count())
		if maxSeqNum < seqNum1 {
			maxSeqNum = seqNum1
		}

		if mem == nil {
			mem = memdb.New(&d.icmpOpts)
		}

		t := b.iter()
		for ; seqNum != seqNum1; seqNum++ {
			kind, ukey, value, ok := t.next()
			if !ok {
				return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
			}
			// Convert seqNum, kind and key into an internalKey, and add that ikey/value
			// pair to mem.
			//
			// TODO: instead of copying to an intermediate buffer (ikey), is it worth
			// adding a SetTwoPartKey(db.TwoPartKey{key0, key1}, value, opts) method to
			// memdb.MemDB? What effect does that have on the db.Comparer interface?
			//
			// The C++ LevelDB code does not need an intermediate copy because its memdb
			// implementation is a private implementation detail, and copies each internal
			// key component from the Batch format straight to the skiplist buffer.
			//
			// Go's LevelDB considers the memdb functionality to be useful in its own
			// right, and so leveldb/memdb is a separate package that is usable without
			// having to import the top-level leveldb package. That extra abstraction
			// means that we need to copy to an intermediate buffer here, to reconstruct
			// the complete internal key to pass to the memdb.
			ikey = makeInternalKey(ikey, ukey, kind, seqNum)
			mem.Set(ikey, value, nil)
		}
		if len(t) != 0 {
			return 0, fmt.Errorf("leveldb: corrupt log file %q", filename)
		}

		// TODO: if mem is large enough, write it to a level-0 table and set mem = nil.

		batchBuf.Reset()
	}

	if mem != nil && !mem.Empty() {
		// 如果初始化后memtable中有数据，则存入磁盘level0层级文件中
		meta, err := d.writeLevel0Table(fs, mem)
		if err != nil {
			return 0, err
		}
		ve.newFiles = append(ve.newFiles, newFileEntry{level: 0, meta: meta})
		// Strictly speaking, it's too early to delete meta.fileNum from d.pendingOutputs,
		// but we are replaying the log file, which happens before Open returns, so there
		// is no possibility of deleteObsoleteFiles being called concurrently here.
		delete(d.pendingOutputs, meta.fileNum)
	}

	return maxSeqNum, nil
}

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

// writeLevel0Table writes a memtable to a level-0 on-disk table.
//
// If no error is returned, it adds the file number of that on-disk table to
// d.pendingOutputs. It is the caller's responsibility to remove that fileNum
// from that set when it has been applied to d.versions.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) writeLevel0Table(fs db.FileSystem, mem *memdb.MemDB) (meta fileMetadata, err error) {
	// meta用于记录新创建的level0 db文件的元信息
	meta.fileNum = d.versions.nextFileNum()
	// filename：新db文件的文件名
	filename := dbFilename(d.dirname, fileTypeTable, meta.fileNum)

	d.pendingOutputs[meta.fileNum] = struct{}{}
	defer func(fileNum uint64) {
		// 如果异常退出(err不为nil)，则从d.pendingOutputs中删除新db文件的记录
		// 否则d.pendingOutputs是用来干什么的呢？
		if err != nil {
			delete(d.pendingOutputs, fileNum)
		}
	}(meta.fileNum)

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	var (
		file db.File
		tw   *table.Writer
		iter db.Iterator
	)
	defer func() {
		if iter != nil {
			err = firstError(err, iter.Close())
		}
		if tw != nil {
			err = firstError(err, tw.Close())
		}
		if file != nil {
			err = firstError(err, file.Close())
		}
		if err != nil {
			fs.Remove(filename)
			meta = fileMetadata{}
		}
	}()

	file, err = fs.Create(filename)
	if err != nil {
		return fileMetadata{}, err
	}

	// table为磁盘db文件封装写入方式
	tw = table.NewWriter(file, &db.Options{
		Comparer: d.icmp,
	})

	// Find返回一个迭代器，用于遍历mem（这里即是d.imm）中的数据
	// memtable是以skiplist来组织数据的，有序
	// 所以取到的第一个数据的key即为当前imm中最小的key
	iter = mem.Find(nil, nil)
	iter.Next()
	// meta.smallest记录新db文件中最小的内部key，在写入memtable时就已经将用户key封装成了内部key，那么为什么还要使用internalKey来做类型转换？
	// 下面的meta.largest就没有进行类型转换就调用clone()方法了。
	meta.smallest = internalKey(iter.Key()).clone()
	for {
		// 最后一次循环中的key即为最大的key，但为什么不封装成内部key呢？
		meta.largest = iter.Key()
		// 将key、value写到新db文件中
		if err1 := tw.Set(meta.largest, iter.Value(), nil); err1 != nil {
			return fileMetadata{}, err1
		}
		// 如果imm中的数据已经遍历完，全部存入新db文件，则break
		if !iter.Next() {
			break
		}
	}
	meta.largest = meta.largest.clone()

	if err1 := iter.Close(); err1 != nil {
		iter = nil
		return fileMetadata{}, err1
	}
	iter = nil

	if err1 := tw.Close(); err1 != nil {
		tw = nil
		return fileMetadata{}, err1
	}
	tw = nil

	// TODO: currently, closing a table.Writer closes its underlying file.
	// We have to re-open the file to Sync or Stat it, which seems stupid.
	file, err = fs.Open(filename)
	if err != nil {
		return fileMetadata{}, err
	}

	if err1 := file.Sync(); err1 != nil {
		return fileMetadata{}, err1
	}

	if stat, err1 := file.Stat(); err1 != nil {
		return fileMetadata{}, err1
	} else {
		size := stat.Size()
		if size < 0 {
			return fileMetadata{}, fmt.Errorf("leveldb: table file %q has negative size %d", filename, size)
		}
		// 将文件的大小值存入meta.size
		meta.size = uint64(size)
	}

	// TODO: compaction stats.

	/* 此时，meta的四个成员：
		- filenum
		- smallest
		- largest
		- size
	  已经全部填上了
	*/
	return meta, nil
}

// makeRoomForWrite ensures that there is room in d.mem for the next write.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) makeRoomForWrite(force bool) error {
	// allowDelay：允许延迟写
	// force = true表示强制立即写？
	allowDelay := !force
	// 无条件循环
	for {
		// TODO: check any previous sticky error, if the paranoid option is set.

		// 若当前版本（即链表versions的最后一个元素）中level0的文件数大于l0SlowdownWritesTrigger(8)，则对当前写操作延迟1毫秒
		if allowDelay && len(d.versions.currentVersion().files[0]) > l0SlowdownWritesTrigger {
			// We are getting close to hitting a hard limit on the number of
			// L0 files. Rather than delaying a single write by several
			// seconds when we hit the hard limit, **start delaying each
			// individual write by 1ms to reduce latency variance.**
			d.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			d.mu.Lock()
			allowDelay = false
			// TODO: how do we ensure we are still 'at the front of the writer queue'?
			continue
		}

		// 若当前memtable中仍有写入的空间，则直接break返回

		// d.mem.ApproximateMemoryUsage()返回len(d.mem.kvData)
		// d.opts.GetWriteBufferSize是memtable的临界大小，也是一个level0的db文件的临界大小
		// 但从 <= 可以看到，这个临界大小是可以被超过的。。。
		if !force && d.mem.ApproximateMemoryUsage() <= d.opts.GetWriteBufferSize() {
			// There is room in the current memtable.
			break
		}

		// 如果当前memtable（即d.mem）已写满，且immutable memtable(d.imm)存在，则需等待，因为说明上一次从imm到level0的compaction操作还没有结束
		// 这也意味着从imm到level0的compaction过程结束时会将d.imm置为nil
		// 而什么时候d.imm会变成非nil呢？见下面，当d.mem满时，会将d.imm指向d.mem，而d.mem则指向一个新申请的内存空间
		// 那么d.imm的存在时间最长为新d.mem从空到被写满的时间
		if d.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			// d.compactionCond.Wait()类似于在等待一个唤醒的信号
			d.compactionCond.Wait()
			continue
		}

		// 若当前版本（即链表versions的最后一个元素）中level0的文件数大于l0StopWritesTrigger(12)，则需等待从level0到level1的compaction过程结束
		if len(d.versions.currentVersion().files[0]) > l0StopWritesTrigger {
			// There are too many level-0 files.
			d.compactionCond.Wait()
			continue
		}

		// Attempt to switch to a new memtable and trigger compaction of old
		// 因为d.mem已经写满，而d.imm也已经被置为nil，那么可以将d.imm指向d.mem,并为d.mem申请一块新的内存空间
		// 从以下代码中可以看到在从d.mem切换到d.imm之前，先创建了一个新的log文件，并打开。这说明一个log文件的生命周期和d.mem是一样的，
		// 即一个log记录的一个d.mem上的增删改操作
		// TODO: drop and re-acquire d.mu around the I/O.

		// 不同的的log文件，通过文件名中不同的数字序号来区分
		newLogNumber := d.versions.nextFileNum()
		newLogFile, err := d.opts.GetFileSystem().Create(dbFilename(d.dirname, fileTypeLog, newLogNumber))
		if err != nil {
			return err
		}
		newLog := record.NewWriter(newLogFile)
		if err := d.log.Close(); err != nil {
			newLogFile.Close()
			return err
		}
		if err := d.logFile.Close(); err != nil {
			newLog.Close()
			newLogFile.Close()
			return err
		}
		// 设置d(DB)的属性
		// 切换到新的日志，新的imm和mem
		d.logNumber, d.logFile, d.log = newLogNumber, newLogFile, newLog
		// memdb.New申请一个新的memtable内存空间
		d.imm, d.mem = d.mem, memdb.New(&d.icmpOpts)
		force = false

		// 由于这时d.imm不为nil，则应该调度一次compaction，将d.imm中的数据写到level0磁盘文件中
		d.maybeScheduleCompaction()
	}
	return nil
}

// deleteObsoleteFiles deletes those files that are no longer needed.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) deleteObsoleteFiles() {
	liveFileNums := map[uint64]struct{}{}
	for fileNum := range d.pendingOutputs {
		liveFileNums[fileNum] = struct{}{}
	}
	d.versions.addLiveFileNums(liveFileNums)
	logNumber := d.versions.logNumber
	manifestFileNumber := d.versions.manifestFileNumber

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	fs := d.opts.GetFileSystem()
	list, err := fs.List(d.dirname)
	if err != nil {
		// Ignore any filesystem errors.
		return
	}
	for _, filename := range list {
		fileType, fileNum, ok := parseDBFilename(filename)
		if !ok {
			return
		}
		keep := true
		switch fileType {
		case fileTypeLog:
			// TODO: also look at prevLogNumber?
			keep = fileNum >= logNumber
		case fileTypeManifest:
			keep = fileNum >= manifestFileNumber
		case fileTypeTable, fileTypeOldFashionedTable:
			_, keep = liveFileNums[fileNum]
		}
		if keep {
			continue
		}
		if fileType == fileTypeTable {
			// 从db.tableCache中将该文件释放掉
			d.tableCache.evict(fileNum)
		}
		// Ignore any file system errors.
		// 删除过期的文件
		fs.Remove(filepath.Join(d.dirname, filename))
	}
}
