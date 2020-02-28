package std

import (
	"fmt"
	s "github.com/matang28/go-streams"
	"io/ioutil"
	"net/http"
	"time"
)

type httpPollingSource struct {
	*s.PollingSource
	client  http.Client
	request http.Request
}

func NewHttpPollingSource(request http.Request, interval time.Duration) *httpPollingSource {
	out := &httpPollingSource{request: request, client: http.Client{}}
	poll := s.NewPollingSource(interval, func(latestCommit string) (entries []s.Entry, e error) {
		if resp, err := out.client.Do(&out.request); err != nil {
			return nil, err
		} else {
			if resp.StatusCode >= 400 {
				return nil,
					fmt.Errorf("%s request to %s returned error code: %d", request.Method, request.URL, resp.StatusCode)
			}

			defer resp.Body.Close()
			if bytes, err := ioutil.ReadAll(resp.Body); err != nil {
				return nil, err
			} else {
				return []s.Entry{
					{Key: fmt.Sprintf("%s-%d", out.request.RequestURI, time.Now().UTC().Unix()), Value: bytes},
				}, nil
			}
		}
	})
	out.PollingSource = poll
	return out
}
