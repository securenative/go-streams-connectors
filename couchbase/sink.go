package couchbase

import (
	"fmt"
	"time"

	s "github.com/matang28/go-streams"
	"gopkg.in/couchbase/gocb.v1"
)

type WriteMethod int

const (
	IGNORE    WriteMethod = 1
	UPSERT    WriteMethod = 2
	REPLACE   WriteMethod = 3
	N1QLQUERY WriteMethod = 4
)

type couchbaseSink struct {
	config SinkConfig

	cluster  *gocb.Cluster
	bucket   *gocb.Bucket
	query    *gocb.N1qlQuery
	singleCh chan errAndKey
	batchCh  chan errAndKey
}

func NewCouchbaseSink(config SinkConfig) *couchbaseSink {
	out := &couchbaseSink{
		config:   config,
		singleCh: make(chan errAndKey, 1),
		batchCh:  make(chan errAndKey),
	}

	if config.Query != "" {
		out.query = gocb.NewN1qlQuery(config.Query)
		out.query.AdHoc(config.QueryAdHoc)
		out.query.Consistency(config.QueryConsistency)
	}

	if err := out.connect(); err != nil {
		panic(err)
	}
	return out
}

func (this *couchbaseSink) Single(entry s.Entry) error {
	go this.writeSingle(entry, this.singleCh)
	for {
		select {
		case err := <-this.singleCh:
			if err.Error != nil {
				return s.NewSinkError(err.Error)
			} else {
				return nil
			}
		case <-time.After(1 * time.Second):
			return s.NewSinkError(fmt.Errorf("timeout when trying to write entry: %+v to couchbase", entry))
		}
	}
}

func (this *couchbaseSink) Batch(entry ...s.Entry) error {
	for idx := range entry {
		go this.writeSingle(entry[idx], this.batchCh)
	}

	successes := make(map[string]bool)
	errs := s.NewSinkBatchError()
	for i := 0; i < len(entry); i++ {
		select {
		case err := <-this.batchCh:
			successes[err.Key] = true
			errs.Add(err.Key, err.Error)
		case <-time.After(1 * time.Second):
			continue
		}
	}

	for idx := range entry {
		_, inSuccess := successes[entry[idx].Key]
		_, inError := errs.Errors[entry[idx].Key]
		if !inSuccess && !inError {
			errs.Add(entry[idx].Key, fmt.Errorf("timeout when trying to write entry: %+v to couchbase", entry[idx]))
		}
	}

	return errs.AsError()
}

func (this *couchbaseSink) writeSingle(entry s.Entry, ch chan<- errAndKey) {
	key := this.config.KeyExtractor(entry)
	ttl := this.config.ExpiryExtractor(entry)

	switch this.config.WriteMethod {
	case IGNORE:
		err := this.executeWithRetries(func() error {
			_, err := this.bucket.Insert(key, entry.Value, ttl)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	case UPSERT:
		err := this.executeWithRetries(func() error {
			_, err := this.bucket.Upsert(key, entry.Value, ttl)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	case N1QLQUERY:
		err := this.executeWithRetries(func() error {
			_, err := this.bucket.ExecuteN1qlQuery(this.query, entry.Value)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	case REPLACE:
		err := this.executeWithRetries(func() error {
			_, err := this.bucket.Replace(key, entry.Value, 0, ttl)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	default:
		panic(fmt.Errorf(
			"unsupported write method: %d, should be one of the following: IGNORE(1), UPSERT(2) or REPLACE(3)",
			this.config.WriteMethod),
		)
	}

	ch <- errAndKey{Key: entry.Key, Error: nil}
}

func (this *couchbaseSink) Ping() error {
	res, err := this.bucket.Ping(this.config.UsedServices)
	if err != nil {
		return err
	}

	for _, ser := range res.Services {
		if !ser.Success {
			return fmt.Errorf("failed to ping service: %+v", ser)
		}
	}

	return nil
}

func (this *couchbaseSink) connect() error {
	cluster, err := gocb.Connect(this.config.Hosts)
	if err != nil {
		return err
	}
	this.cluster = cluster

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: this.config.Username,
		Password: this.config.Password,
	})
	if err != nil {
		return err
	}

	bucket, err := cluster.OpenBucket(this.config.Bucket, this.config.BucketPassword)
	if err != nil {
		return err
	}
	this.bucket = bucket

	return this.Ping()
}

func (this *couchbaseSink) executeWithRetries(fn RetryFunc) error {
	var err error
	var maxRetries = 1
	if this.config.MaxRetries > 0 {
		maxRetries = this.config.MaxRetries
	}

	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil
		} else {
			s.Log().Warn("Failed to execute query against couchbase (%d/%d), failed with error: %s",
				i, this.config.RetryTimeout, err.Error())
		}

		time.Sleep(this.config.RetryTimeout * time.Duration(i+1))
	}

	return err
}

type RetryFunc func() error

type errAndKey struct {
	Error error
	Key   string
}
