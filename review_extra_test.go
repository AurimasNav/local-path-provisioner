package main

// Review-only tests (not for upstream). They exercise readyHandler behind a
// real HTTP server with a kubelet-like probe client.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// hangingAPI returns a clientset whose first request hangs until the client
// goes away (or maxHang passes); later requests answer an empty list.
func hangingAPI(t *testing.T, maxHang time.Duration) (clientset.Interface, *atomic.Int32, chan struct{}) {
	t.Helper()
	var calls atomic.Int32
	cancelled := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			select {
			case <-r.Context().Done():
				close(cancelled)
				return
			case <-time.After(maxHang):
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{},"items":[]}`))
	}))
	t.Cleanup(api.Close)
	kubeClient, err := clientset.NewForConfig(&rest.Config{Host: api.URL})
	require.NoError(t, err)
	return kubeClient, &calls, cancelled
}

// kubeletLikeClient mirrors k8s.io/kubernetes/pkg/probe/http: a new
// connection per probe and http.Client.Timeout as the probe timeout.
func kubeletLikeClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DisableKeepAlives:  true,
			Proxy:              http.ProxyURL(nil),
			DisableCompression: true,
		},
	}
}

func TestReviewKubeletLikeProbeTimeout(t *testing.T) {
	kubeClient, calls, apiCancelled := hangingAPI(t, 30*time.Second)

	handlerDone := make(chan struct{})
	probeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		readyHandler(kubeClient, "default", w, r)
	}))
	t.Cleanup(probeSrv.Close)

	start := time.Now()
	resp, err := kubeletLikeClient(time.Second).Get(probeSrv.URL + "/ready")
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	t.Logf("probe client error after %v: %v", time.Since(start), err)

	select {
	case <-apiCancelled:
		t.Logf("in-flight API call cancelled %v after probe start", time.Since(start))
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight API call was not cancelled after the probe client timed out")
	}
	select {
	case <-handlerDone:
		t.Logf("handler returned %v after probe start", time.Since(start))
	case <-time.After(5 * time.Second):
		t.Fatal("readyHandler kept running after the probe client timed out")
	}
	require.EqualValues(t, 1, calls.Load(), "API requests made")
}

func TestReviewDeadlineForClientThatNeverGivesUp(t *testing.T) {
	kubeClient, calls, _ := hangingAPI(t, 60*time.Second)

	probeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readyHandler(kubeClient, "default", w, r)
	}))
	t.Cleanup(probeSrv.Close)

	start := time.Now()
	resp, err := kubeletLikeClient(0).Get(probeSrv.URL + "/ready")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	elapsed := time.Since(start)
	t.Logf("status %d after %v, body: %q", resp.StatusCode, elapsed, string(body))

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Contains(t, string(body), "readiness check configmaps failed")
	require.Contains(t, string(body), "context deadline exceeded")
	require.GreaterOrEqual(t, elapsed, readinessCheckTimeout-500*time.Millisecond)
	require.Less(t, elapsed, readinessCheckTimeout+5*time.Second)
	require.EqualValues(t, 1, calls.Load(), "API requests made")
}
