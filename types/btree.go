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
	return binary.LittleEndian.Uint16(node[:2])
}

func (node Node) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node Node) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node Node) getPtr(idx uint16) uint64 {
	pos := HEADER_SIZE + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node Node) setPtr(idx uint16, val uint64) {
	pos := HEADER_SIZE + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func offsetPos(node Node, idx uint16) uint16 {
	return HEADER_SIZE + 8*node.nkeys() + 2*(idx-1)
}

func (node Node) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node Node) setOffset(idx uint16, offset uint16) {
	if idx == 0 {
		return
	}

	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node Node) kvPos(idx uint16) uint16 {
  return HEADER_SIZE + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node Node) getKey(idx uint16) []byte {
  pos := node.kvPos(idx)
  klen := binary.LittleEndian.Uint16(node[pos:])
  return node[pos+4:][:klen]
}

func (node Node) getVal(idx uint16) []byte {
  pos := node.kvPos(idx)
  klen := binary.LittleEndian.Uint16(node[pos:])
  vlen := binary.LittleEndian.Uint16(node[pos+klen:])
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
