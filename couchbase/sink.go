package couchbase

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/pkg/errors"
	"time"

	s "github.com/matang28/go-streams"
)

type WriteMethod int

const (
	IGNORE           WriteMethod = 1
	UPSERT           WriteMethod = 2
	REPLACE          WriteMethod = 3
	N1QLQUERY        WriteMethod = 4 // on this case you must pass the object as map
	MUTATE_OR_INSERT WriteMethod = 5
)

type couchbaseSink struct {
	config SinkConfig

	cluster  *gocb.Cluster
	bucket   *gocb.Bucket
	timeout  time.Duration
	singleCh chan errAndKey
	batchCh  chan errAndKey
}

func NewCouchbaseSink(config SinkConfig) *couchbaseSink {
	out := &couchbaseSink{
		config:   config,
		singleCh: make(chan errAndKey, 1),
		batchCh:  make(chan errAndKey),
	}

	// get max execution time with retries and operation timeout
	a1 := config.Timeout + config.RetryTimeout
	an := a1 + time.Duration(config.MaxRetries-1)*config.RetryTimeout
	out.timeout = ((a1 + an) * time.Duration(config.MaxRetries)) / 2

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
		case <-time.After(this.timeout):
			return s.NewSinkError(fmt.Errorf("timeout when trying to write entry: %+v to couchbase", entry))
		}
	}
}

func (this *couchbaseSink) Batch(entry ...s.Entry) error {
	if this.config.GroupByKey {
		m := make(map[string][]s.Entry)
		for _, item := range entry {
			key := this.config.KeyExtractor(item)
			//init array per key
			if _, exists := m[key]; !exists {
				m[key] = []s.Entry{}
			}
			m[key] = append(m[key], item)
		}

		for _, values := range m {
			go func(items []s.Entry) {
				for _, item := range items {
					this.writeSingle(item, this.batchCh)
				}
			}(values)
		}
	} else {
		for idx := range entry {
			go this.writeSingle(entry[idx], this.batchCh)
		}
	}

	successes := make(map[string]bool)
	errs := s.NewSinkBatchError()
	for i := 0; i < len(entry); i++ {
		select {
		case err := <-this.batchCh:
			successes[err.Key] = true
			errs.Add(err.Key, err.Error)
		case <-time.After(this.timeout):
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
	expiry := this.config.ExpiryExtractor(entry)

	switch this.config.WriteMethod {
	case IGNORE:
		opts := &gocb.InsertOptions{Expiry: expiry, Timeout: this.config.Timeout}
		err := this.executeWithRetries(func() error {
			_, err := this.bucket.DefaultCollection().Insert(key, entry.Value, opts)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	case UPSERT:
		opts := &gocb.UpsertOptions{Expiry: expiry, Timeout: this.config.Timeout}
		err := this.executeWithRetries(func() error {
			_, err := this.bucket.DefaultCollection().Upsert(key, entry.Value, opts)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	case N1QLQUERY:
		opts := &gocb.QueryOptions{
			NamedParameters: entry.Value.(map[string]interface{}),
			Adhoc:           this.config.QueryAdHoc,
			ScanConsistency: this.config.QueryConsistency,
			Timeout:         this.config.Timeout,
		}
		err := this.executeWithRetries(func() error {
			_, err := this.cluster.Query(this.config.Query, opts)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	case REPLACE:
		opts := &gocb.ReplaceOptions{Cas: 0, Expiry: expiry, Timeout: this.config.Timeout}
		err := this.executeWithRetries(func() error {
			_, err := this.bucket.DefaultCollection().Replace(key, entry.Value, opts)
			return err
		})
		if err != nil {
			ch <- errAndKey{Key: entry.Key, Error: err}
			return
		}
	case MUTATE_OR_INSERT:
		mutateOpts := &gocb.MutateInOptions{Timeout: this.config.Timeout}
		insertOpts := &gocb.InsertOptions{Expiry: expiry, Timeout: this.config.Timeout}
		err := this.executeWithRetries(func() error {
			mutateOps, insertObject, err := this.config.MutateOpsExtractor(entry)
			if err != nil {
				return errors.Wrap(err, "failed to extract ops during mutation")
			}
			_, err = this.bucket.DefaultCollection().MutateIn(key, mutateOps, mutateOpts)
			if err != nil && errors.Is(err, gocb.ErrDocumentNotFound) {
				// do an insert if the key does not exists
				_, err = this.bucket.DefaultCollection().Insert(key, insertObject, insertOpts)
			}
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
	res, err := this.bucket.Ping(&gocb.PingOptions{
		ServiceTypes: this.config.UsedServices,
	})
	if err != nil {
		return err
	}

	for _, serviceResults := range res.Services {
		for _, result := range serviceResults {
			if result.State != gocb.PingStateOk {
				return fmt.Errorf("failed to ping service, error: %s", result.Error)
			}
		}
	}

	return nil
}

func (this *couchbaseSink) connect() error {
	cluster, err := gocb.Connect(this.config.Hosts, gocb.ClusterOptions{
		Username: this.config.Username,
		Password: this.config.Password,
	})
	if err != nil {
		return err
	}
	this.cluster = cluster

	bucket := cluster.Bucket(this.config.Bucket)
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
