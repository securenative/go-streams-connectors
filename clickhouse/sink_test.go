package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go"
	s "github.com/matang28/go-streams"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClickhouseSink_Single(t *testing.T) {
	createTable()

	entry := s.Entry{
		Key:   "test",
		Value: Record{time.Now().UTC(), "test post", 10233, clickhouse.Array([]string{"tag1", "tag2", "tag3"})},
	}

	err := sink.Single(entry)
	assert.Nil(t, err)

	records := readAll()
	assert.EqualValues(t, 1, len(records))
	assertEntry(t, entry, records[0])

	dropTable()
}

func TestClickhouseSink_Batch(t *testing.T) {
	createTable()

	entry1 := s.Entry{
		Key:   "test1",
		Value: Record{time.Now(), "test post1", 10233, clickhouse.Array([]string{"tag1", "tag2", "tag3"})},
	}

	entry2 := s.Entry{
		Key:   "test2",
		Value: Record{time.Now(), "test post2", 21312, clickhouse.Array([]string{"tag1"})},
	}

	entry3 := s.Entry{
		Key:   "test3",
		Value: Record{time.Now(), "test post3", 2, clickhouse.Array([]string{})},
	}

	err := sink.Batch(entry1, entry2, entry3)
	assert.Nil(t, err)

	records := readAll()
	assert.EqualValues(t, 3, len(records))
	assertEntry(t, entry1, records[0])
	assertEntry(t, entry2, records[1])
	assertEntry(t, entry3, records[2])
	dropTable()
}

func TestClickhouseSink_genPlaceholders(t *testing.T) {
	assert.EqualValues(t, "", genPlaceholders(0))
	assert.EqualValues(t, "?", genPlaceholders(1))
	assert.EqualValues(t, "?,?", genPlaceholders(2))
	assert.EqualValues(t, "?,?,?", genPlaceholders(3))
	assert.EqualValues(t, "?,?,?,?,?,?,?,?,?,?", genPlaceholders(10))
}

func TestClickhouseSink_genInsertQuery(t *testing.T) {
	var query string

	query = genInsertQuery("test", RecordMapping{"timestamp"})
	assert.EqualValues(t, "INSERT INTO test (timestamp) VALUES (?)", query)

	query = genInsertQuery("test", RecordMapping{"timestamp", "id", "count"})
	assert.EqualValues(t, "INSERT INTO test (timestamp,id,count) VALUES (?,?,?)", query)
}

func assertEntry(t *testing.T, expected s.Entry, actual Record) {
	assert.EqualValues(t, expected.Value.(Record)[0].(time.Time).Unix(), actual[0].(time.Time).Unix()) // timestamp
	assert.EqualValues(t, expected.Value.(Record)[1], actual[1])                                       // name
	assert.EqualValues(t, expected.Value.(Record)[2], actual[2])                                       // likes
	assert.EqualValues(t, expected.Value.(Record)[3], actual[3])                                       // tags
}
