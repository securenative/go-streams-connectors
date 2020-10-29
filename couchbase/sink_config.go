package couchbase

import (
	"time"

	"github.com/couchbase/gocb/v2"
	s "github.com/matang28/go-streams"
)

// useful when we are using maps and want to pass the key using the map elements
//
// for example on n1ql queries we need to pass a map for the parameters, the easy way is to have a model that
// has a parameter called key and we extract the key for the query using this parameter and this extractor
var MapElementKeyExtractor = func(elementName string) s.KeyExtractor {
	return func(entry s.Entry) string {
		doc := entry.Value.(map[string]interface{})
		return doc[elementName].(string)
	}
}

// just extracting the key from the entry key
var EntryKeyExtractor = func(entry s.Entry) string {
	return entry.Key
}

type ExpiryExtractor func(entry s.Entry) (expiry time.Duration)

var NoExpiry ExpiryExtractor = func(entry s.Entry) (expiry time.Duration) {
	return 0
}

// when we mutate we need to get the operations for mutation and if we need to do an insert we need to
// have the object (can be map) that we want to insert, we pass this function and it will get these
// things for us on each entry
type MutateOpsExtractor func(entry s.Entry) (mutateOps []gocb.MutateInSpec, insertObject interface{}, err error)

type SinkConfig struct {
	Hosts            string
	Username         string
	Password         string
	BucketPassword   string
	Bucket           string
	Query            string
	QueryConsistency gocb.QueryScanConsistency
	QueryAdHoc       bool
	GroupByKey       bool
	MaxRetries       int
	RetryTimeout     time.Duration
	Timeout          time.Duration
	UsedServices     []gocb.ServiceType
	WriteMethod      WriteMethod

	KeyExtractor       s.KeyExtractor
	ExpiryExtractor    ExpiryExtractor
	MutateOpsExtractor MutateOpsExtractor
}

func NewSinkConfig(hosts string, username string, password string, bucketPassword string, bucket string) SinkConfig {
	out := SinkConfig{
		Hosts:            hosts,
		Username:         username,
		Password:         password,
		BucketPassword:   bucketPassword,
		Bucket:           bucket,
		QueryConsistency: gocb.QueryScanConsistencyRequestPlus,
		QueryAdHoc:       true,
		GroupByKey:       false,
		MaxRetries:       5,
		RetryTimeout:     10 * time.Millisecond,
		Timeout:          1 * time.Second,
	}

	out.WriteMethod = UPSERT

	out.UsedServices = []gocb.ServiceType{gocb.ServiceTypeQuery, gocb.ServiceTypeKeyValue}

	out.KeyExtractor = EntryKeyExtractor

	out.ExpiryExtractor = NoExpiry

	return out
}
