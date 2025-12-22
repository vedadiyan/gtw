package gtw

import (
	"testing"
	"time"
)

// badReader returns (0, nil) repeatedly - violates io.Reader contract
type badReader struct {
	callCount int
}

func (b *badReader) Read(p []byte) (int, error) {
	b.callCount++
	return 0, nil // Misbehaving: returns (0, nil) forever
}

func (b *badReader) Close() error {
	return nil
}

func TestInfiniteLoopWithBadReader(t *testing.T) {
	bad := &badReader{}
	mrc := MultipleReadCloser(bad)

	done := make(chan struct{})
	buf := make([]byte, 100)
	var n int
	var err error

	n, err = mrc.Read(buf)
	close(done)

	select {
	case <-done:
		t.Logf("Read returned: n=%d, err=%v, callCount=%d", n, err, bad.callCount)
		if bad.callCount > 1000 {
			t.Errorf("Infinite loop detected: reader called %d times", bad.callCount)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("INFINITE LOOP CONFIRMED: Reader called %d times in 1 second", bad.callCount)
	}
}
