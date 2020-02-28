package kafka

import (
	s "github.com/matang28/go-streams"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKafkaSink_Single(t *testing.T) {
	entry := s.Entry{
		Key:   "entry1",
		Value: []byte("entry value 1"),
	}

	err := sink.Single(entry)
	assert.Nil(t, err)

	messages := readN(1)
	assert.EqualValues(t, entry.Key, messages[0].Key)
	assert.EqualValues(t, entry.Value, messages[0].Value)
}

func TestKafkaSink_Batch(t *testing.T) {
	entry1 := s.Entry{
		Key:   "entry1",
		Value: []byte("entry value 1"),
	}
	entry2 := s.Entry{
		Key:   "entry2",
		Value: []byte("entry value 2"),
	}
	entry3 := s.Entry{
		Key:   "entry3",
		Value: []byte("entry value 3"),
	}

	err := sink.Batch(entry1, entry2, entry3)
	assert.Nil(t, err)

	messages := readN(3)
	assert.EqualValues(t, entry1.Key, messages[0].Key)
	assert.EqualValues(t, entry1.Value, messages[0].Value)

	assert.EqualValues(t, entry2.Key, messages[1].Key)
	assert.EqualValues(t, entry2.Value, messages[1].Value)

	assert.EqualValues(t, entry3.Key, messages[2].Key)
	assert.EqualValues(t, entry3.Value, messages[2].Value)
}
