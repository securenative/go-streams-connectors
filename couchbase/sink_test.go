package couchbase

import (
	go_streams "github.com/matang28/go-streams"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCouchbaseSink_Single(t *testing.T) {
	entry := go_streams.Entry{
		Key: "entry1",
		Value: model{
			Name:    "Suman Sumani",
			Age:     33,
			Hobbies: []string{"Football", "Basketball", "Running"},
		},
	}

	err := sink.Single(entry)
	assert.Nil(t, err)

	var actual model
	read(entry.Key, &actual)
	assert.EqualValues(t, entry.Value, actual)
}

func TestCouchbaseSink_Batch(t *testing.T) {
	entry1 := go_streams.Entry{
		Key: "entry1",
		Value: model{
			Name:    "Suman Sumani",
			Age:     33,
			Hobbies: []string{"Football", "Basketball", "Running"},
		},
	}

	entry2 := go_streams.Entry{
		Key: "entry2",
		Value: model{
			Name:    "Duda dudicai",
			Age:     22,
			Hobbies: []string{"Basketball", "Running"},
		},
	}

	entry3 := go_streams.Entry{
		Key: "entry3",
		Value: model{
			Name:    "Kuku Kukaki",
			Age:     16,
			Hobbies: []string{},
		},
	}

	err := sink.Batch(entry1, entry2, entry3)
	assert.Nil(t, err)

	var actual model
	read(entry1.Key, &actual)
	assert.EqualValues(t, entry1.Value, actual)
	read(entry2.Key, &actual)
	assert.EqualValues(t, entry2.Value, actual)
	read(entry3.Key, &actual)
	assert.EqualValues(t, entry3.Value, actual)
}

type model struct {
	Name    string
	Age     int
	Hobbies []string
}
