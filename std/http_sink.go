package std

import (
	"fmt"
	s "github.com/matang28/go-streams"
	"net/http"
	"time"
)

type RequestFactory func(entry s.Entry) http.Request

type httpSink struct {
	client         http.Client
	requestFactory RequestFactory
}

func NewHttpSink(requestFactory RequestFactory, timeout time.Duration) *httpSink {
	return &httpSink{requestFactory: requestFactory, client: http.Client{Timeout: timeout}}
}

func (this *httpSink) Ping() error {
	return nil
}

func (this *httpSink) Single(entry s.Entry) error {
	ch := make(chan errAndKey, 1)
	this.do(entry, ch)
	err := <-ch
	return err.err
}

func (this *httpSink) Batch(entry ...s.Entry) error {
	ch := make(chan errAndKey, len(entry))
	for idx := range entry {
		go this.do(entry[idx], ch)
	}
	err := s.NewSinkBatchError()
	for i := 0; i < len(entry); i++ {
		e := <-ch
		err.Add(e.key, e.err)
	}
	return err.AsError()
}

func (this *httpSink) do(entry s.Entry, errs chan errAndKey) {
	request := this.requestFactory(entry)
	if resp, err := this.client.Do(&request); err != nil {
		errs <- errAndKey{err: err, key: entry.Key}
	} else {
		if resp.StatusCode >= 400 {
			errs <- errAndKey{
				err: fmt.Errorf("%s request to %s returned error code: %d", request.Method, request.URL, resp.StatusCode),
				key: entry.Key,
			}
		}
	}
	errs <- errAndKey{
		err: nil,
		key: entry.Key,
	}
}

type errAndKey struct {
	err error
	key string
}
