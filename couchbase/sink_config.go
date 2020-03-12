package couchbase

import (
	s "github.com/matang28/go-streams"
	"gopkg.in/couchbase/gocb.v1"
)

type ExpiryExtractor func(entry s.Entry) (ttlSeconds uint32)

type SinkConfig struct {
	Hosts          string
	Username       string
	Password       string
	BucketPassword string
	Bucket         string
	Query          string
	UsedServices   []gocb.ServiceType
	WriteMethod    WriteMethod

	KeyExtractor    s.KeyExtractor
	ExpiryExtractor ExpiryExtractor
}

func NewSinkConfig(hosts string, username string, password string, bucketPassword string, bucket string) SinkConfig {
	out := SinkConfig{
		Hosts:          hosts,
		Username:       username,
		Password:       password,
		BucketPassword: bucketPassword,
		Bucket:         bucket,
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
