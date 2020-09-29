package couchbase

import (
	"time"

	s "github.com/matang28/go-streams"
	"gopkg.in/couchbase/gocb.v1"
)

type ExpiryExtractor func(entry s.Entry) (ttlSeconds uint32)

type SinkConfig struct {
	Hosts            string
	Username         string
	Password         string
	BucketPassword   string
	Bucket           string
	Query            string
	QueryConsistency gocb.ConsistencyMode
	QueryAdHoc       bool
	GroupByKey       bool
	MaxRetries       int
	RetryTimeout     time.Duration
	Timeout          time.Duration
	UsedServices     []gocb.ServiceType
	WriteMethod      WriteMethod

	KeyExtractor    s.KeyExtractor
	ExpiryExtractor ExpiryExtractor
}

func NewSinkConfig(hosts string, username string, password string, bucketPassword string, bucket string) SinkConfig {
	out := SinkConfig{
		Hosts:            hosts,
		Username:         username,
		Password:         password,
		BucketPassword:   bucketPassword,
		Bucket:           bucket,
		QueryConsistency: gocb.RequestPlus,
		QueryAdHoc:       true,
		GroupByKey:       false,
		MaxRetries:       5,
		RetryTimeout:     10 * time.Millisecond,
		Timeout:          1 * time.Second,
	}

	out.WriteMethod = UPSERT

	out.UsedServices = []gocb.ServiceType{}

	out.KeyExtractor = func(entry s.Entry) string {
		return entry.Key
	}

	out.ExpiryExtractor = func(entry s.Entry) (ttlSeconds uint32) {
		return 0
	}

	return out
}
