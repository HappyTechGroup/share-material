// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
    "encoding/binary"
)

const batchHeaderLen = 12

// 为什么
const invalidBatchCount = 1<<32 - 1

// Batch is a sequence of Sets and/or Deletes that are applied atomically.
type Batch struct {
    // Data is the wire format of a batch's log entry:
    //     a sequence number of the first batch element?
    //   - 8 bytes for a sequence number of the first batch element,
    //     or zeroes if the batch has not yet been applied,
    //   - 4 bytes for the count: the number of elements in the batch,
    //     or "\xff\xff\xff\xff" if the batch is invalid,
    //   - count elements, being:
    //     - one byte for the kind: delete (0) or set (1),
    //     - the varint-string user key,
    //     - the varint-string value (if kind == set).
    // The sequence number and count are stored in little-endian order.
	/*
		data的编码方式：
			- 8个字节用于存储batch的第一个元素的序列号，当batch还没被应用时初始化为0
			- 4个字节用于存储batch中元素的数目，当这4个字节的值为"\xff\xff\xff\xff"时，表示该batch无效，所以一个batch最多可以存入2^32 - 2个元素
			- 接下来存储：
				- 操作的类型：0表示删除，1表示设置（新增或更新）
				- 用户提供的key
				- 用户提供的value（如果是set操作）
		因为每个操作都会记录在日志中，日志中的条目格式也是这样的
	*/
    data []byte
}

// Set adds an action to the batch that sets the key to map to the value.
func (b *Batch) Set(key, value []byte) {
	// 如果还没有Batch中还没有数据，则为b.data申请一块初始内存空间，空间大小为 >= len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen
	// binary.MaxVarintLen64 = 10，2 x binary.MaxVarintLen64的空间是用于存储key和value的长度的
	// 但数据是从b.data的batchHeaderLen长度之后开始写的，batchHeaderLen长度的空间是用来在将该batch应用到memtable之前写入头部元信息的
    if len(b.data) == 0 {
        b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen)
    }
	// 将用于元素计数的data[8:12]的值加1
    if b.increment() {
		// 将代表操作类型(set或delete)的data[12]置为1，表示当前操作为set
        b.data = append(b.data, byte(internalKeyKindSet))
		// 然后设置key和value，设置方式是先存入key或value的长度，然后存入key或value
        b.appendStr(key)
        b.appendStr(value)
    }
}

// Delete adds an action to the batch that deletes the entry for key.
func (b *Batch) Delete(key []byte) {
    if len(b.data) == 0 {
		// 因为没有value，所以只需binary.MaxVarintLen64来存储key的长度
        b.init(len(key) + binary.MaxVarintLen64 + batchHeaderLen)
    }
    if b.increment() {
        b.data = append(b.data, byte(internalKeyKindDelete))
		// 对于删除(delete)操作来说，是没有value，所以只需存入key
        b.appendStr(key)
    }
}

func (b *Batch) init(cap int) {
    n := 256
    for n < cap {
        n *= 2
    }
    b.data = make([]byte, batchHeaderLen, n)
}

// seqNumData returns the 8 byte little-endian sequence number. Zero means that
// the batch has not yet been applied.
func (b *Batch) seqNumData() []byte {
    return b.data[:8]
}

// countData returns the 4 byte little-endian count data. "\xff\xff\xff\xff"
// means that the batch is invalid.
func (b *Batch) countData() []byte {
    return b.data[8:12]
}

// 这是为什么呢？
func (b *Batch) increment() (ok bool) {
	// b.countData()返回data中代表batch中记录数目的部分，b.data[8:12]
    p := b.countData()
	// 对计数加1，由于返回的是字节串，所以是从低位到高位逐个加1，直到当前的位加1后不为0为止（为0，表示需要进位，即高一位需要加1）
	// 如果直到最后一位都没有return true，则说明当前的batch.set使得data超出的容量，是无效的，则将这计数的的4位都置为0xff，表示无效的batch
    for i := range p {
        p[i]++
        if p[i] != 0x00 {
            return true
        }
    }
    // The countData was "\xff\xff\xff\xff". Leave it as it was.
	// 用"\xff\xff\xff\xff"来表示无效的batch，貌似有点问题啊，如果真的元素个数达到xffxffxffxff个，那么就自动变成无效的batch了。
    p[0] = 0xff
    p[1] = 0xff
    p[2] = 0xff
    p[3] = 0xff
    return false
}

func (b *Batch) appendStr(s []byte) {
	// binary.MaxVarintLen64 = 10
    var buf [binary.MaxVarintLen64]byte
	// PutUvarint encodes a uint64 into buf and returns the number of bytes written. If the buffer is too small, PutUvarint will panic.
    n := binary.PutUvarint(buf[:], uint64(len(s)))
    b.data = append(b.data, buf[:n]...)
    b.data = append(b.data, s...)
}

func (b *Batch) setSeqNum(seqNum uint64) {
    binary.LittleEndian.PutUint64(b.seqNumData(), seqNum)
}

func (b *Batch) seqNum() uint64 {
    return binary.LittleEndian.Uint64(b.seqNumData())
}

func (b *Batch) count() uint32 {
    return binary.LittleEndian.Uint32(b.countData())
}

func (b *Batch) iter() batchIter {
    return b.data[batchHeaderLen:]
}

type batchIter []byte

// next returns the next operation in this batch.
// The final return value is false if the batch is corrupt.
func (t *batchIter) next() (kind internalKeyKind, ukey []byte, value []byte, ok bool) {
    p := *t
    if len(p) == 0 {
        return 0, nil, nil, false
    }
    kind, *t = internalKeyKind(p[0]), p[1:]
    if kind > internalKeyKindMax {
        return 0, nil, nil, false
    }
    ukey, ok = t.nextStr()
    if !ok {
        return 0, nil, nil, false
    }
    if kind != internalKeyKindDelete {
        value, ok = t.nextStr()
        if !ok {
            return 0, nil, nil, false
        }
    }
    return kind, ukey, value, true
}

func (t *batchIter) nextStr() (s []byte, ok bool) {
    p := *t
    u, numBytes := binary.Uvarint(p)
    if numBytes <= 0 {
        return nil, false
    }
    p = p[numBytes:]
    if u > uint64(len(p)) {
        return nil, false
    }
    s, *t = p[:u], p[u:]
    return s, true
}
