package spdk

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestCreateDoesNotHoldLockWhileSendingUpdate(c *C) {
	ef := NewEngineFrontend("ef-create-lock", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, make(chan interface{}))

	errCh := make(chan error, 1)
	go func() {
		_, err := ef.Create(nil, "127.0.0.1:9500")
		errCh <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for ef.Get().State != string(lhtypes.InstanceStateRunning) {
		if time.Now().After(deadline) {
			c.Fatal("timeout waiting for engine frontend to enter running state")
		}
		time.Sleep(10 * time.Millisecond)
	}

	getDone := make(chan struct{}, 1)
	go func() {
		_ = ef.Get()
		getDone <- struct{}{}
	}()

	select {
	case <-getDone:
		// expected: Create is blocked on UpdateCh but lock is already released
	case <-time.After(1 * time.Second):
		c.Fatal("Get() blocked while Create is waiting on UpdateCh; lock may still be held")
	}

	select {
	case <-ef.UpdateCh:
		// unblock Create()
	case <-time.After(1 * time.Second):
		c.Fatal("timeout waiting for Create() update signal")
	}

	select {
	case err := <-errCh:
		c.Assert(err, IsNil)
	case <-time.After(1 * time.Second):
		c.Fatal("timeout waiting for Create() to return")
	}
}

func (s *TestSuite) TestConcurrentCreateHasSingleWinner(c *C) {
	ef := NewEngineFrontend("ef-create-concurrent", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, make(chan interface{}, 32))

	const workers = 20
	startCh := make(chan struct{})
	var wg sync.WaitGroup
	var successCount int32
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			_, err := ef.Create(nil, "127.0.0.1:9500")
			if err == nil {
				atomic.AddInt32(&successCount, 1)
				return
			}
			errCh <- err
		}()
	}

	close(startCh)
	wg.Wait()
	close(errCh)

	c.Assert(successCount, Equals, int32(1))
	c.Assert(len(errCh), Equals, workers-1)

	for err := range errCh {
		c.Assert(err, NotNil)
		errStr := err.Error()
		c.Assert(strings.Contains(errStr, "already creating") || strings.Contains(errStr, "invalid state"), Equals, true,
			Commentf("unexpected concurrent create error: %v", err))
	}

	c.Assert(ef.Get().State, Equals, string(lhtypes.InstanceStateRunning))
}
