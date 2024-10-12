// BTree/Node definition module
//
// Node structure:
//
// A fixed-size header, which contains:
//
// - The type of node (leaf or internal) [1]
// - The number of keys [2]
// - A list of pointers to child nodes for internal nodes [3]
// - A list of offsets to KVs, which can be used to binary search KVs [4]
// - A list of KV pairs [5]
//
// | type | nkeys |  pointers  |   offsets  | key-values | unused |
// |  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |
//
// This is the format of each KV pair. Lengths followed by data [6]
//
// | klen | vlen | key | val |
// |  2B  |  2B  | ... | ... |
package types

import (
	"bytes"
	"encoding/binary"
)

const HEADER_SIZE = 4

const (
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

const (
	BNODE_INTERNAL_TYPE = iota
	BNODE_LEAF_TYPE
)

type Node []byte

type BTree struct {
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
	root uint64
}

func (node Node) btype() uint16 {
	// refer to [1]
	return binary.LittleEndian.Uint16(node[:2])
}

func (node Node) nkeys() uint16 {
	// refer to [2]
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node Node) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// Returns 64-bit (8B) pointers to child nodes, see [3]
func (node Node) getPtr(idx uint16) uint64 {
	pos := HEADER_SIZE + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

// Sets 64-bit pointer given an index number
func (node Node) setPtr(idx uint16, val uint64) {
	pos := HEADER_SIZE + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// Returns offset 0-based index at the offsets section, see [4].
// It accepts a 1-based, 2B position value
// This is because the offset index here is being measured from its end offset, e.g.
//
//	start offset for kv1
//	|         start offset for kv2
//	|         |
//	v         v
//	0         1         2         3
//
// ...|___kv1___|___kv2___|___kv3___|...
//
//	|         |         |         |
//	          ^
//	          |
//	          end offset kv1
//
// Since the start offset for kv1 is 0
//
//	the end offset for kv1 is 1,
//	the end offset for kv2 is 2,
//	...
//	the end offset for kvN is N
//
// Restriction: 1 <= pos <= node.nkeys()
func offsetIdx(node Node, pos uint16) uint16 {
	return HEADER_SIZE + 8*node.nkeys() + 2*(pos-1)
}

// Returns KV pair offset value
// It accepts a 1-based, 2B position value
func (node Node) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[offsetIdx(node, idx):])
}

// Sets offset value to KV pair
// Restriction: 1 <= pos <= node.nkeys()
func (node Node) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetIdx(node, idx):], offset)
}

// Returns KV pair position relative to the whole node, see [5].
//
// It accepts an 1-based position number
// Restriction: idx <= node.nkeys()
func (node Node) kvPos(idx uint16) uint16 {
	return HEADER_SIZE + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// Returns key, see [6]
func (node Node) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])

	// skip key value size
	return node[pos+4:][:klen]
}

// Returns value, see [6]
func (node Node) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+klen:])

	// skip key and value sizes
	return node[pos+4:][klen:][:vlen]
}

func (node Node) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func nodeLookupLE(node Node, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)

		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}

	return found
}

func nodeLookupEQ(node Node, key []byte) uint16 {
	nkeys := node.nkeys()

	for i := uint16(1); i < nkeys; i++ {
		if bytes.Equal(node.getKey(i), key) {
			return i
		}
	}

	return 0
}

// Copies a range of KV pairs
//
// new Node: new Node KV pairs are to be copied to
// old Node: old Node KV pairs are to be copied from
// dstNew uint16: new node's copy range initial position
// srcOld uint16: old node's copy range initial position
// n uint16: copy range offset/size
func nodeAppendRange(new Node, old Node, dstNew, srcOld, n uint16) {
	dstNewPos := new.kvPos(srcOld)
	srcOldPos := old.kvPos(dstNew)
	copy(new[dstNewPos:dstNewPos+n], old[srcOldPos:srcOldPos+n])
}

// Copies a KV into position
func nodeAppendKV(new Node, idx uint16, ptr uint64, key []byte, val []byte) {
	// init pointers
	new.setPtr(idx, ptr)

	// inits kv header
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	// actually copies data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	// sets offset for the next key, skipping key and value sizes
	// order is guaranteed
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

func leafInsert(new Node, old Node, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF_TYPE, old.nkeys()+1) // sets header

	// inserts first KV pairs
	nodeAppendRange(new, old, 0, 0, idx)

	// sets ptr to 0, since it's a leaf insert
	nodeAppendKV(new, idx, 0, key, val)

	// inserts remaining KV pairs
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}
