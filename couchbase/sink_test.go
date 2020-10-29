package couchbase

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/gocb/v2"
	go_streams "github.com/matang28/go-streams"
	"github.com/stretchr/testify/assert"
	"testing"
)

type model struct {
	Name    string   `json:"name"`
	Age     int      `json:"age"`
	Hobbies []string `json:"hobbies"`
}

type keyedModel struct {
	Key     string   `json:"key"`
	Name    string   `json:"name"`
	Age     int      `json:"age"`
	Hobbies []string `json:"hobbies"`
}

const insertQuery = `insert into %s (KEY,VALUE) VALUES ($key, {"Name": $name, "Age": $age, "Hobbies": $hobbies})`

func TestCouchbaseSink_upsert(t *testing.T) {
	// Single
	entry := go_streams.Entry{
		Key: "entry1",
		Value: model{
			Name:    "Suman Sumani",
			Age:     33,
			Hobbies: []string{"Football", "Basketball", "Running"},
		},
	}

	err := sink.Single(entry)
	assert.NoError(t, err)

	var actual model
	read(entry.Key, &actual)
	assert.EqualValues(t, entry.Value, actual)

	// Batch
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

	err = sink.Batch(entry1, entry2, entry3)
	assert.NoError(t, err)

	read(entry1.Key, &actual)
	assert.EqualValues(t, entry1.Value, actual)
	read(entry2.Key, &actual)
	assert.EqualValues(t, entry2.Value, actual)
	read(entry3.Key, &actual)
	assert.EqualValues(t, entry3.Value, actual)
}

// run insert query and validate stored data
func TestCouchbaseSink_n1ql(t *testing.T) {
	sink.config.QueryConsistency = gocb.QueryScanConsistencyRequestPlus
	sink.config.QueryAdHoc = true
	sink.config.WriteMethod = N1QLQUERY
	sink.config.KeyExtractor = MapElementKeyExtractor("key")

	// Single - insert query
	sink.config.Query = fmt.Sprintf(insertQuery, testConfig.Bucket)
	model1 := keyedModel{
		Key:     "insert1",
		Name:    "Suman Sumani",
		Age:     11,
		Hobbies: []string{"Football", "Basketball", "Running"},
	}
	modelAsMap, err := toMap(model1)
	assert.NoError(t, err)
	entry1 := go_streams.Entry{Key: "1", Value: modelAsMap}

	err = sink.Single(entry1)
	assert.NoError(t, err)

	var actual keyedModel
	read("insert1", &actual)
	assert.Equal(t, model1.Name, actual.Name)
	assert.Equal(t, model1.Age, actual.Age)
	assert.Equal(t, model1.Hobbies, actual.Hobbies)

	// Batch - insert
	model2 := keyedModel{
		Key:     "insert2",
		Name:    "Suman 1212",
		Age:     22,
		Hobbies: []string{"Football"},
	}
	modelAsMap, err = toMap(model2)
	assert.NoError(t, err)
	entry2 := go_streams.Entry{Key: "2", Value: modelAsMap}

	model3 := keyedModel{
		Key:     "insert3",
		Name:    "Suman 1313",
		Age:     33,
		Hobbies: []string{"Football", "Running"},
	}
	modelAsMap, err = toMap(model3)
	assert.NoError(t, err)
	entry3 := go_streams.Entry{Key: "3", Value: modelAsMap}

	model4 := keyedModel{
		Key:     "insert4",
		Name:    "Suman 1414",
		Age:     44,
		Hobbies: []string{"Football", "Basketball"},
	}
	modelAsMap, err = toMap(model4)
	assert.NoError(t, err)
	entry4 := go_streams.Entry{Key: "4", Value: modelAsMap}

	err = sink.Batch(entry2, entry3, entry4)
	assert.NoError(t, err)

	read("insert2", &actual)
	assert.Equal(t, model2.Name, actual.Name)
	assert.Equal(t, model2.Age, actual.Age)
	assert.Equal(t, model2.Hobbies, actual.Hobbies)

	read("insert3", &actual)
	assert.Equal(t, model3.Name, actual.Name)
	assert.Equal(t, model3.Age, actual.Age)
	assert.Equal(t, model3.Hobbies, actual.Hobbies)

	read("insert4", &actual)
	assert.Equal(t, model4.Name, actual.Name)
	assert.Equal(t, model4.Age, actual.Age)
	assert.Equal(t, model4.Hobbies, actual.Hobbies)
}

func TestCouchbaseSink_replace(t *testing.T) {
	sink.config.WriteMethod = UPSERT
	sink.config.KeyExtractor = EntryKeyExtractor

	// insert objects and check initial values are inserted
	entry1 := go_streams.Entry{
		Key: "replace1",
		Value: model{
			Name:    "Suman Sumani 1",
			Age:     11,
			Hobbies: []string{"Football", "Running"},
		},
	}

	entry2 := go_streams.Entry{
		Key: "replace2",
		Value: model{
			Name:    "Suman Sumani 2",
			Age:     22,
			Hobbies: []string{"Football", "Basketball", "Running"},
		},
	}

	err := sink.Batch(entry1, entry2)
	assert.NoError(t, err)

	var actual model
	read(entry1.Key, &actual)
	assert.EqualValues(t, entry1.Value, actual)
	read(entry2.Key, &actual)
	assert.EqualValues(t, entry2.Value, actual)

	// Single - insert, check initial values are inserted, replace, check replaced values
	sink.config.WriteMethod = REPLACE

	entry1Replaced := go_streams.Entry{
		Key: "replace1",
		Value: model{
			Name:    "Suman Sumani 1 replaced",
			Age:     110,
			Hobbies: []string{"Football"},
		},
	}

	err = sink.Single(entry1Replaced)
	assert.NoError(t, err)

	read(entry1Replaced.Key, &actual)
	assert.EqualValues(t, entry1Replaced.Value, actual)

	// Batch
	entry1Replaced = go_streams.Entry{
		Key: "replace1",
		Value: model{
			Name:    "Suman Sumani 1 replaced",
			Age:     1100,
			Hobbies: []string{"Football"},
		},
	}

	entry2Replaced := go_streams.Entry{
		Key: "replace2",
		Value: model{
			Name:    "Suman Sumani 2 replaced",
			Age:     220,
			Hobbies: []string{"Football", "Basketball", "Running"},
		},
	}

	err = sink.Batch(entry1Replaced, entry2Replaced)
	assert.NoError(t, err)

	read(entry1Replaced.Key, &actual)
	assert.EqualValues(t, entry1Replaced.Value, actual)
	read(entry2Replaced.Key, &actual)
	assert.EqualValues(t, entry2Replaced.Value, actual)
}

func TestCouchbaseSink_mutate_or_insert(t *testing.T) {
	sink.config.WriteMethod = MUTATE_OR_INSERT
	sink.config.KeyExtractor = func(entry go_streams.Entry) string {
		return entry.Value.(keyedModel).Key
	}

	// insert or mutate ALL model params - using struct
	sink.config.MutateOpsExtractor = func(entry go_streams.Entry) (mutateOps []gocb.MutateInSpec, insertObject interface{}, err error) {
		m := entry.Value.(keyedModel)
		insertObject = model{m.Name, m.Age, m.Hobbies}
		mutateOps = []gocb.MutateInSpec{
			gocb.ReplaceSpec("name", m.Name, nil),
			gocb.ReplaceSpec("age", m.Age, nil),
			gocb.ReplaceSpec("hobbies", m.Hobbies, nil),
		}
		return
	}
	runInsertOrMutateCheckSingle(t, "1")

	// insert or mutate ALL model params - using map
	sink.config.MutateOpsExtractor = func(entry go_streams.Entry) (mutateOps []gocb.MutateInSpec, insertObject interface{}, err error) {
		m := entry.Value.(keyedModel)
		insertObject = map[string]interface{}{"name": m.Name, "age": m.Age, "hobbies": m.Hobbies}
		mutateOps = []gocb.MutateInSpec{
			gocb.ReplaceSpec("name", m.Name, nil),
			gocb.ReplaceSpec("age", m.Age, nil),
			gocb.ReplaceSpec("hobbies", m.Hobbies, nil),
		}
		return
	}
	runInsertOrMutateCheckSingle(t, "2")

	// insert or mutate ALL model params - using map - second option
	sink.config.MutateOpsExtractor = func(entry go_streams.Entry) (mutateOps []gocb.MutateInSpec, insertObject interface{}, err error) {
		m := entry.Value.(keyedModel)
		insertObject, err = toMap(m)
		if err != nil {
			return
		}
		// remove the key parameter we don't want it in the actual data object
		delete(insertObject.(map[string]interface{}), "key")
		mutateOps = []gocb.MutateInSpec{
			gocb.ReplaceSpec("name", m.Name, nil),
			gocb.ReplaceSpec("age", m.Age, nil),
			gocb.ReplaceSpec("hobbies", m.Hobbies, nil),
		}
		return
	}
	runInsertOrMutateCheckSingle(t, "3")

	// insert full data but mutate only one parameter
	sink.config.MutateOpsExtractor = func(entry go_streams.Entry) (mutateOps []gocb.MutateInSpec, insertObject interface{}, err error) {
		m := entry.Value.(keyedModel)
		insertObject, err = toMap(m)
		if err != nil {
			return
		}
		// remove the key parameter we don't want it in the actual data object
		delete(insertObject.(map[string]interface{}), "key")
		mutateOps = []gocb.MutateInSpec{
			gocb.ReplaceSpec("name", m.Name, nil), // if we are mutating then only change this one (for test)
		}
		return
	}

	model4 := keyedModel{
		Key:     "moi4",
		Name:    "Suman Sumani first",
		Age:     11,
		Hobbies: []string{"Football", "Basketball", "Running"},
	}
	entry4 := go_streams.Entry{Key: "4", Value: model4}

	err := sink.Single(entry4)
	assert.NoError(t, err)

	var actual model
	read("moi4", &actual)
	assert.Equal(t, model4.Name, actual.Name)
	assert.Equal(t, model4.Age, actual.Age)
	assert.Equal(t, model4.Hobbies, actual.Hobbies)

	// mutate
	// since we are mutating and know that the extractor only needs the Name parameter
	// we don't have to specify all parameters
	model4Mutated := keyedModel{
		Key:     "moi4",
		Name:    "Suman Sumani mutated",
	}
	entry4Mutated := go_streams.Entry{Key: "4", Value: model4Mutated}

	err = sink.Single(entry4Mutated)
	assert.NoError(t, err)

	read("moi4", &actual)
	assert.Equal(t, model4Mutated.Name, actual.Name) // this was updated
	assert.Equal(t, model4.Age, actual.Age)          // this is old
	assert.Equal(t, model4.Hobbies, actual.Hobbies)  // this is old

	//
	//
	//
	// batch - some are mutated and some new and some not included and are unchanged
	//

	sink.config.MutateOpsExtractor = func(entry go_streams.Entry) (mutateOps []gocb.MutateInSpec, insertObject interface{}, err error) {
		m := entry.Value.(keyedModel)
		insertObject, err = toMap(m)
		if err != nil {
			return
		}
		// remove the key parameter we don't want it in the actual data object
		delete(insertObject.(map[string]interface{}), "key")
		mutateOps = []gocb.MutateInSpec{
			gocb.ReplaceSpec("name", m.Name, nil),
			gocb.ReplaceSpec("age", m.Age, nil),
			gocb.ReplaceSpec("hobbies", m.Hobbies, nil),
		}
		return
	}

	// 1 - mutated
	model1 := keyedModel{
		Key:     "moi1",
		Name:    "Suman Sumani mutated batch",
		Age:     119,
		Hobbies: []string{"Football", "Running"},
	}
	entry1 := go_streams.Entry{Key: "1", Value: model1}

	// 3 - mutated
	model3 := keyedModel{
		Key:     "moi3",
		Name:    "Suman Sumani mutated batch",
		Age:     339,
		Hobbies: []string{"Football"},
	}
	entry3 := go_streams.Entry{Key: "3", Value: model3}

	// 5 - new
	model5 := keyedModel{
		Key:     "moi5",
		Name:    "Suman Sumani new batch",
		Age:     339,
		Hobbies: []string{"Football", "New"},
	}
	entry5 := go_streams.Entry{Key: "5", Value: model5}

	err = sink.Batch(entry1, entry3, entry5)
	assert.NoError(t, err)

	// read 1 - mutated
	read("moi1", &actual)
	assert.Equal(t, model1.Name, actual.Name)
	assert.Equal(t, model1.Age, actual.Age)
	assert.Equal(t, model1.Hobbies, actual.Hobbies)

	// read 2 - not changed
	read("moi2", &actual)
	assert.Equal(t, "Suman Sumani mutated", actual.Name)
	assert.Equal(t, 1122, actual.Age)
	assert.Equal(t, []string{"Football"}, actual.Hobbies)

	// read 3 - mutated
	read("moi3", &actual)
	assert.Equal(t, model3.Name, actual.Name)
	assert.Equal(t, model3.Age, actual.Age)
	assert.Equal(t, model3.Hobbies, actual.Hobbies)

	// read 4 - not changed
	read("moi4", &actual)
	assert.Equal(t, model4Mutated.Name, actual.Name)
	assert.Equal(t, model4.Age, actual.Age)
	assert.Equal(t, model4.Hobbies, actual.Hobbies)

	// read 5 - new
	read("moi5", &actual)
	assert.Equal(t, model5.Name, actual.Name)
	assert.Equal(t, model5.Age, actual.Age)
	assert.Equal(t, model5.Hobbies, actual.Hobbies)
}

func runInsertOrMutateCheckSingle(t *testing.T, id string) {
	// insert
	model1 := keyedModel{
		Key:     "moi" + id,
		Name:    "Suman Sumani first",
		Age:     11,
		Hobbies: []string{"Football", "Basketball", "Running"},
	}
	entry1 := go_streams.Entry{Key: id, Value: model1}

	err := sink.Single(entry1)
	assert.NoError(t, err)

	var actual model
	read("moi"+id, &actual)
	assert.Equal(t, model1.Name, actual.Name)
	assert.Equal(t, model1.Age, actual.Age)
	assert.Equal(t, model1.Hobbies, actual.Hobbies)

	// mutate
	model1 = keyedModel{
		Key:     "moi" + id,
		Name:    "Suman Sumani mutated",
		Age:     1122,
		Hobbies: []string{"Football"},
	}
	entry1 = go_streams.Entry{Key: id, Value: model1}

	err = sink.Single(entry1)
	assert.NoError(t, err)

	read("moi"+id, &actual)
	assert.Equal(t, model1.Name, actual.Name)
	assert.Equal(t, model1.Age, actual.Age)
	assert.Equal(t, model1.Hobbies, actual.Hobbies)
}

func toMap(in interface{}) (map[string]interface{}, error) {
	var res map[string]interface{}
	marshaled, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(marshaled, &res); err != nil {
		return nil, err
	}
	return res, nil
}
