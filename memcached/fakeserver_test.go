package memcached

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer speaks the subset of the memcached text protocol gomemcache uses:
// gets, set, add, cas, delete and incr, with relative expirations on an injectable clock.
type fakeServer struct {
	ln net.Listener

	mu      sync.Mutex
	now     time.Time
	data    map[string]fakeItem
	nextCAS uint64
}

type fakeItem struct {
	val    []byte
	flags  string
	expiry time.Time
	cas    uint64
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeServer{ln: ln, now: time.Unix(1_700_000_000, 0), data: map[string]fakeItem{}}
	go fs.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return fs
}

func (fs *fakeServer) addr() string { return fs.ln.Addr().String() }

func (fs *fakeServer) advance(d time.Duration) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.now = fs.now.Add(d)
}

func (fs *fakeServer) serve() {
	for {
		c, err := fs.ln.Accept()
		if err != nil {
			return
		}
		go fs.handle(c)
	}
}

func (fs *fakeServer) handle(c net.Conn) {
	defer c.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
	for {
		line, err := rw.ReadString('\n')
		if err != nil {
			return
		}
		f := strings.Fields(line)
		if len(f) == 0 {
			return
		}
		fs.mu.Lock()
		switch f[0] {
		case "gets":
			for _, k := range f[1:] {
				if it, ok := fs.live(k); ok {
					fmt.Fprintf(rw, "VALUE %s %s %d %d\r\n%s\r\n", k, it.flags, len(it.val), it.cas, it.val)
				}
			}
			rw.WriteString("END\r\n")
		case "set", "add", "cas":
			n, _ := strconv.Atoi(f[4])
			buf := make([]byte, n+2)
			fs.mu.Unlock()
			if _, err := io.ReadFull(rw, buf); err != nil {
				return
			}
			fs.mu.Lock()
			rw.WriteString(fs.store(f, buf[:n]))
		case "delete":
			if _, ok := fs.live(f[1]); ok {
				delete(fs.data, f[1])
				rw.WriteString("DELETED\r\n")
			} else {
				rw.WriteString("NOT_FOUND\r\n")
			}
		case "incr":
			it, ok := fs.live(f[1])
			delta, _ := strconv.ParseUint(f[2], 10, 64)
			n, err := strconv.ParseUint(string(it.val), 10, 64)
			switch {
			case !ok:
				rw.WriteString("NOT_FOUND\r\n")
			case err != nil:
				rw.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
			default:
				n += delta
				it.val = []byte(strconv.FormatUint(n, 10))
				fs.nextCAS++
				it.cas = fs.nextCAS
				fs.data[f[1]] = it
				fmt.Fprintf(rw, "%d\r\n", n)
			}
		default:
			rw.WriteString("ERROR\r\n")
		}
		fs.mu.Unlock()
		if err := rw.Flush(); err != nil {
			return
		}
	}
}

// live returns an unexpired item. Caller holds mu.
func (fs *fakeServer) live(k string) (fakeItem, bool) {
	it, ok := fs.data[k]
	if ok && !it.expiry.IsZero() && !fs.now.Before(it.expiry) {
		delete(fs.data, k)
		return fakeItem{}, false
	}
	return it, ok
}

// store handles set/add/cas. Caller holds mu.
func (fs *fakeServer) store(f []string, val []byte) string {
	k := f[1]
	cur, present := fs.live(k)
	switch f[0] {
	case "add":
		if present {
			return "NOT_STORED\r\n"
		}
	case "cas":
		want, _ := strconv.ParseUint(f[5], 10, 64)
		if !present {
			return "NOT_FOUND\r\n"
		}
		if cur.cas != want {
			return "EXISTS\r\n"
		}
	}
	exp, _ := strconv.Atoi(f[3])
	it := fakeItem{val: bytes.Clone(val), flags: f[2]}
	if exp > 0 {
		it.expiry = fs.now.Add(time.Duration(exp) * time.Second)
	}
	fs.nextCAS++
	it.cas = fs.nextCAS
	fs.data[k] = it
	return "STORED\r\n"
}
