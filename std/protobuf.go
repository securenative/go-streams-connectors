package std

import (
	"github.com/golang/protobuf/proto"
	s "github.com/matang28/go-streams"
	"reflect"
)

func ToProtoBytes(entry interface{}) interface{} {
	protobuf := entry.(proto.Message)
	if bytes, err := proto.Marshal(protobuf); err != nil {
		panic(err)
	} else {
		return bytes
	}
}

func ToProtoString(entry interface{}) interface{} {
	return string(ToProtoBytes(entry).([]byte))
}

func FromProto(protoMessage proto.Message) s.MapFunc {
	return func(entry interface{}) interface{} {
		switch e := entry.(type) {
		case []byte:
			out := reflect.New(reflect.TypeOf(protoMessage)).Interface().(proto.Message)
			if err := proto.Unmarshal(e, out); err != nil {
				panic(err)
			}
			return out
		case string:
			out := reflect.New(reflect.TypeOf(protoMessage)).Interface().(proto.Message)
			if err := proto.UnmarshalText(e, out); err != nil {
				panic(err)
			}
			return out
		default:
			panic("FromProto can only accept strings or []byte")
		}
	}
}
