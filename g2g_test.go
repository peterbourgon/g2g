package g2g

import (
	"testing"
	"net"
	"time"
	"expvar"
	"sync"
	"strings"
)

type MockGraphite struct {
	t     *testing.T
	count int
	mtx   sync.Mutex
	ln    net.Listener
	done  chan bool
}

func NewMockGraphite(t *testing.T) *MockGraphite {
	m := &MockGraphite{
		t:     t,
		count: 0,
		mtx:   sync.Mutex{},
		ln:    nil,
		done:  make(chan bool, 1),
	}
	go m.loop()
	return m
}

func (m *MockGraphite) Count() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.count
}

func (m *MockGraphite) Shutdown() {
	m.ln.Close()
	<-m.done
}

func (m *MockGraphite) loop() {
	ln, err := net.Listen("tcp", ":2003")
	if err != nil {
		panic(err)
	}
	m.ln = ln
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			m.done <- true
			return
		}
		go m.handle(conn)
	}
}

func (m *MockGraphite) handle(conn net.Conn) {
	b := make([]byte, 1024)
	for {
		n, err := conn.Read(b)
		if err != nil {
			m.t.Logf("Mock Graphite: read error: %s", err)
			return
		}
		if n > 256 {
			m.t.Errorf("Mock Graphite: read %dB: too much data", n)
			return
		}
		s := strings.TrimSpace(string(b[:n]))
		m.t.Logf("Mock Graphite: read %dB: %s", n, s)
		m.mtx.Lock()
		m.count++
		m.mtx.Unlock()
	}
}

func TestPublish(t *testing.T) {

	// setup
	mock := NewMockGraphite(t)
	d := 25 * time.Millisecond
	g, err := NewGraphite("localhost:2003", d, d)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// register, wait, check
	i := expvar.NewInt("i")
	i.Set(34)
	g.Register("test.foo.i", i)

	time.Sleep(2 * d)
	count := mock.Count()
	if !(0 < count && count <= 2) {
		t.Errorf("expected 0 < publishes <= 2, got %d", count)
	}
	t.Logf("after %s, count=%d", 2*d, count)

	time.Sleep(2 * d)
	count = mock.Count()
	if !(2 < count && count <= 4) {
		t.Errorf("expected 2 < publishes <= 4, got %d", count)
	}
	t.Logf("after second %s, count=%d", 2*d, count)

	// teardown
	ok := make(chan bool)
	go func() {
		g.Shutdown()
		mock.Shutdown()
		ok <- true
	}()
	select {
	case <-ok:
		t.Logf("shutdown OK")
	case <-time.After(d):
		t.Errorf("timeout during shutdown")
	}

}
