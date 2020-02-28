package kafka

import (
	s "github.com/matang28/go-streams"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKafkaSource_Start(t *testing.T) {
	entry1 := s.Entry{
		Key:   "0-entry1",
		Value: []byte("entry value 1"),
	}
	entry2 := s.Entry{
		Key:   "1-entry2",
		Value: []byte("entry value 2"),
	}
	entry3 := s.Entry{
		Key:   "2-entry3",
		Value: []byte("entry value 3"),
	}

	err := sinkSourceTopic.Batch(entry1, entry2, entry3)
	panicOnErr(err)

	var actual []s.Entry
	for i := 0; i < 3; i++ {
		e, ok := <-ch
		if !ok {
			break
		}
		actual = append(actual, e)
	}

	assert.EqualValues(t, 3, len(actual))

	assert.EqualValues(t, entry1.Key, actual[0].Key)
	assert.EqualValues(t, entry1.Value, actual[0].Value)

	assert.EqualValues(t, entry2.Key, actual[1].Key)
	assert.EqualValues(t, entry2.Value, actual[1].Value)

	assert.EqualValues(t, entry3.Key, actual[2].Key)
	assert.EqualValues(t, entry3.Value, actual[2].Value)
}
