package utils

import "net/http"

type RemoveHeaderTransport struct {
	T http.RoundTripper
}

func (t *RemoveHeaderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Del("X-Ratelimit-Limit")
	req.Header.Del("X-Ratelimit-Remaining")
	req.Header.Del("X-Ratelimit-Reset")
	return t.T.RoundTrip(req)
}

type RetryTransport struct {
	T http.RoundTripper
}

func (t *RetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var r *http.Response
	var err error
	Retry(func() error {
		r, err = t.T.RoundTrip(req)
		return err
	})

	return r, err
}
