package utils

import (
	"container/ring"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/go-github/github"
	"github.com/gregjones/httpcache"
	"github.com/gregjones/httpcache/diskcache"
	"golang.org/x/oauth2"
)

type WrapperClient struct {
	clients      *ring.Ring
	currentIndex int
}

func (wc *WrapperClient) Request(f func(*github.Client) (interface{}, *github.Response, error)) (interface{}, *github.Response, error) {
	for {
		i := 0
		for ; i < wc.clients.Len(); i++ {
			fmt.Printf("using client with index %d\n", wc.currentIndex)
			v, resp, err := f(wc.getActiveClient())
			if err == nil {
				i = 0
				return v, resp, err
			}

			if ghErr, ok := err.(*github.RateLimitError); ok {
				fmt.Printf("error: RateLimitError with Rate %v\n", ghErr.Rate)
				fmt.Printf("changing client\n")
				wc.changeClient()
				continue
			}

			if ghErr, ok := err.(*github.AbuseRateLimitError); ok {
				fmt.Printf("error: AbuseRateLimitError with RetryAfter %v\n", ghErr.RetryAfter)
				fmt.Printf("changing client\n")
				wc.changeClient()
				continue
			}

			return v, resp, err
		}

		// TODO: set sleepAmount to minimum between all rate limit reset
		sleepAmount := 10 * time.Minute
		fmt.Printf("all clients tested, sleeping for %v\n", sleepAmount)
		time.Sleep(sleepAmount)
	}
}

func (wc *WrapperClient) getActiveClient() *github.Client {
	return wc.clients.Value.(*github.Client)
}

func (wc *WrapperClient) changeClient() {
	wc.clients = wc.clients.Next()
	if wc.currentIndex == wc.clients.Len()-1 {
		wc.currentIndex = 0
	} else {
		wc.currentIndex = wc.currentIndex + 1
	}
}

func NewWrapperClient(tokens []string) (*WrapperClient, error) {
	r := ring.New(len(tokens))

	for _, token := range tokens {
		c, err := newClientNoLimit(token)
		if err != nil {
			return nil, err
		}

		r.Value = c
		r = r.Next()
	}

	return &WrapperClient{clients: r, currentIndex: 0}, nil
}

func newClientNoLimit(token string) (*github.Client, error) {
	http := oauth2.NewClient(context.TODO(), oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	))

	dirPath := filepath.Join(os.TempDir(), "ghsync")
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("error while creating directory %s: %v", dirPath, err)
	}

	t := httpcache.NewTransport(diskcache.New(dirPath))
	t.Transport = &RemoveHeaderTransport{http.Transport}
	http.Transport = &RetryTransport{T: t}

	return github.NewClient(http), nil
}
