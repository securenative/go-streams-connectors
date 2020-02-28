package std

import (
	"encoding/json"
	s "github.com/matang28/go-streams"
	"reflect"
)

func ToJsonBytes(entry interface{}) interface{} {
	if bytes, err := json.Marshal(entry); err != nil {
		panic(err)
	} else {
		return bytes
	}
}

func ToJsonString(entry interface{}) interface{} {
	return string(ToJsonBytes(entry).([]byte))
}

func FromJson(contractPtr interface{}) s.MapFunc {
	return func(entry interface{}) interface{} {
		switch e := entry.(type) {
		case []byte:
			out := reflect.New(reflect.TypeOf(contractPtr)).Interface()
			if err := json.Unmarshal(e, out); err != nil {
				panic(err)
			}
			return out
		case string:
			out := reflect.New(reflect.TypeOf(contractPtr)).Interface()
			if err := json.Unmarshal([]byte(e), out); err != nil {
				panic(err)
			}
			return out
		default:
			panic("FromJson can only accept strings or []byte")
		}
	}
}
