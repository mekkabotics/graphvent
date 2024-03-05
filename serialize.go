package graphvent

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
)

type SerializedType uint64

func (t SerializedType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type ExtType SerializedType

func (t ExtType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type NodeType SerializedType

func (t NodeType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type SignalType SerializedType

func (t SignalType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type FieldTag SerializedType

func (t FieldTag) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

func NodeTypeFor(name string, extensions []ExtType, mappings map[string]FieldIndex) NodeType {
  digest := []byte("GRAPHVENT_NODE[" + name + "] - ")
  for _, ext := range(extensions) {
    digest = binary.BigEndian.AppendUint64(digest, uint64(ext))
  }

  digest = binary.BigEndian.AppendUint64(digest, 0)

  sorted_keys := make([]string, len(mappings))
  i := 0
  for key := range(mappings) {
    sorted_keys[i] = key
    i += 1
  }
  slices.Sort(sorted_keys)



  for _, key := range(sorted_keys) {
    digest = append(digest, []byte(key + ":")...)
    digest = binary.BigEndian.AppendUint64(digest, uint64(mappings[key].Extension))
    digest = append(digest, []byte(mappings[key].Field + "|")...)
  }

  hash := sha512.Sum512(digest)
  return NodeType(binary.BigEndian.Uint64(hash[0:8]))
}

func SerializeType(t reflect.Type) SerializedType {
  digest := []byte(t.String())
  hash := sha512.Sum512(digest)
  return SerializedType(binary.BigEndian.Uint64(hash[0:8]))
}

func SerializedTypeFor[T any]() SerializedType {
  return SerializeType(reflect.TypeFor[T]())
}

func ExtTypeFor[E any, T interface { *E; Extension}]() ExtType {
  return ExtType(SerializedTypeFor[E]())
}

func SignalTypeFor[S Signal]() SignalType {
  return SignalType(SerializedTypeFor[S]())
}

func Hash(base, data string) SerializedType { 
  digest := []byte(base + ":" + data)
  hash := sha512.Sum512(digest)
  return SerializedType(binary.BigEndian.Uint64(hash[0:8]))
}

func GetFieldTag(tag string) FieldTag {
  return FieldTag(Hash("GRAPHVENT_FIELD_TAG", tag))
}
