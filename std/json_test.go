package std

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToJsonBytes(t *testing.T) {
	model := map[string]interface{}{
		"string": "value1",
		"array":  []int{1, 2, 3},
		"nested": map[string]interface{}{
			"one": 1,
			"two": "2",
		},
	}

	res := ToJsonBytes(model)
	bytes, ok := res.([]byte)
	assert.True(t, ok)
	assert.EqualValues(t, string(bytes), `{"array":[1,2,3],"nested":{"one":1,"two":"2"},"string":"value1"}`)
}

func TestToJsonString(t *testing.T) {
	model := map[string]interface{}{
		"string": "value1",
		"array":  []int{1, 2, 3},
		"nested": map[string]interface{}{
			"one": 1,
			"two": "2",
		},
	}

	res := ToJsonString(model)
	str, ok := res.(string)
	assert.True(t, ok)
	assert.EqualValues(t, str, `{"array":[1,2,3],"nested":{"one":1,"two":"2"},"string":"value1"}`)
}

func TestFromJson_Bytes(t *testing.T) {
	bytes := []byte(`{"name":"matan","age": 100}`)
	model := FromJson(Person{})(bytes).(*Person)
	assert.EqualValues(t, "matan", model.Name)
	assert.EqualValues(t, 100, model.Age)
}

func TestFromJson_String(t *testing.T) {
	str := `{"name":"matan","age": 100}`
	model := FromJson(Person{})(str).(*Person)
	assert.EqualValues(t, "matan", model.Name)
	assert.EqualValues(t, 100, model.Age)
}

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}
