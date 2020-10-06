// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package network

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stretchr/testify/assert"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/networking/router"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/version"
)

var (
	errClosed  = errors.New("closed")
	errRefused = errors.New("connection refused")

	ip0Port = 1
	ip1Port = 2
	ip0     = utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: uint16(ip0Port),
	}
	id0 = ids.NewShortID(hashing.ComputeHash160Array([]byte(ip0.String())))
	ip1 = utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: uint16(ip1Port),
	}
	id1               = ids.NewShortID(hashing.ComputeHash160Array([]byte(ip1.String())))
	testNetworkID     = uint32(0)
	testAppVersion    = version.NewDefaultVersion("app", 0, 1, 0)
	testVersionParser = version.NewDefaultParser()
	testUpgrader      = func() Upgrader { return NewIPUpgrader() }
	testVdrs          = validators.NewSet()
)

type testListener struct {
	addr    net.Addr
	inbound chan net.Conn
	once    sync.Once
	closed  chan struct{}
}

func (l *testListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.inbound:
		return c, nil
	case <-l.closed:
		return nil, errClosed
	}
}

func (l *testListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *testListener) Addr() net.Addr { return l.addr }

type testDialer struct {
	addr      net.Addr
	outbounds map[string]*testListener
}

func (d *testDialer) Dial(ip utils.IPDesc) (net.Conn, error) {
	outbound, ok := d.outbounds[ip.String()]
	if !ok {
		return nil, errRefused
	}
	server := &testConn{
		pendingReads:  make(chan []byte, 1<<10),
		pendingWrites: make(chan []byte, 1<<10),
		closed:        make(chan struct{}),
		local:         outbound.addr,
		remote:        d.addr,
	}
	client := &testConn{
		pendingReads:  server.pendingWrites,
		pendingWrites: server.pendingReads,
		closed:        make(chan struct{}),
		local:         d.addr,
		remote:        outbound.addr,
	}

	select {
	case outbound.inbound <- server:
		return client, nil
	default:
		return nil, errRefused
	}
}

type testConn struct {
	partialRead   []byte
	pendingReads  chan []byte
	pendingWrites chan []byte
	closed        chan struct{}
	once          sync.Once

	local, remote net.Addr
}

func (c *testConn) Read(b []byte) (int, error) {
	for len(c.partialRead) == 0 {
		select {
		case read, ok := <-c.pendingReads:
			if !ok {
				return 0, errClosed
			}
			c.partialRead = read
		case <-c.closed:
			return 0, errClosed
		}
	}

	copy(b, c.partialRead)
	if length := len(c.partialRead); len(b) > length {
		c.partialRead = nil
		return length, nil
	}
	c.partialRead = c.partialRead[len(b):]
	return len(b), nil
}

func (c *testConn) Write(b []byte) (int, error) {
	newB := make([]byte, len(b))
	copy(newB, b)

	select {
	case c.pendingWrites <- newB:
	case <-c.closed:
		return 0, errClosed
	}

	return len(b), nil
}

func (c *testConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *testConn) LocalAddr() net.Addr              { return c.local }
func (c *testConn) RemoteAddr() net.Addr             { return c.remote }
func (c *testConn) SetDeadline(time.Time) error      { return nil }
func (c *testConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testConn) SetWriteDeadline(time.Time) error { return nil }

type testHandler struct {
	router.Router
	connected    func(ids.ShortID)
	disconnected func(ids.ShortID)
}

func (h *testHandler) Connected(id ids.ShortID) {
	if h.connected != nil {
		h.connected(id)
	}
}
func (h *testHandler) Disconnected(id ids.ShortID) {
	if h.disconnected != nil {
		h.disconnected(id)
	}
}

// Returns two networks, connected to each other
func testNetwork(t *testing.T) (*sync.WaitGroup, *sync.WaitGroup, *network, *sync.WaitGroup, *sync.WaitGroup, *network) {
	log := logging.NoLog{}
	networkID := uint32(0)
	appVersion := version.NewDefaultVersion("app", 0, 1, 0)
	versionParser := version.NewDefaultParser()

	listener0 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: ip0Port,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller0 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: ip0Port,
		},
		outbounds: make(map[string]*testListener),
	}
	listener1 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: ip1Port,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller1 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: ip1Port,
		},
		outbounds: make(map[string]*testListener),
	}

	caller0.outbounds[ip1.String()] = listener1
	caller1.outbounds[ip0.String()] = listener0

	serverUpgrader := NewIPUpgrader()
	clientUpgrader := NewIPUpgrader()

	vdrs := validators.NewSet()

	var (
		net0Connected    sync.WaitGroup
		net1Connected    sync.WaitGroup
		net0Disconnected sync.WaitGroup
		net1Disconnected sync.WaitGroup
	)

	handler0 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id0) {
				net0Connected.Done()
			}
		},
		disconnected: func(id ids.ShortID) {
			if !id.Equals(id0) {
				net0Disconnected.Done()
			}
		},
	}
	handler1 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				net1Connected.Done()
			}
		},
		disconnected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				net1Disconnected.Done()
			}
		},
	}

	net0 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id0,
		ip0,
		networkID,
		appVersion,
		versionParser,
		listener0,
		caller0,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler0,
	)
	assert.NotNil(t, net0)

	net1 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id1,
		ip1,
		networkID,
		appVersion,
		versionParser,
		listener1,
		caller1,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler1,
	)
	assert.NotNil(t, net1)

	net0.Track(ip1)

	net0Connected.Add(1)
	net1Connected.Add(1)

	go func() {
		err := net0.Dispatch()
		assert.Error(t, err)
	}()
	go func() {
		err := net1.Dispatch()
		assert.Error(t, err)
	}()

	net0Connected.Wait()
	net1Connected.Wait()
	// The two peers are connected

	return &net0Connected, &net0Disconnected, net0.(*network), &net1Connected, &net1Disconnected, net1.(*network)
}

func TestNewDefaultNetwork(t *testing.T) {
	log := logging.NoLog{}
	ip := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 0,
	}
	id := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip.String())))
	networkID := uint32(0)
	appVersion := version.NewDefaultVersion("app", 0, 1, 0)
	versionParser := version.NewDefaultParser()

	listener := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		outbounds: make(map[string]*testListener),
	}
	serverUpgrader := NewIPUpgrader()
	clientUpgrader := NewIPUpgrader()

	vdrs := validators.NewSet()
	handler := &testHandler{}

	net := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id,
		ip,
		networkID,
		appVersion,
		versionParser,
		listener,
		caller,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler,
	)
	assert.NotNil(t, net)

	go func() {
		err := net.Close()
		assert.NoError(t, err)
	}()

	err := net.Dispatch()
	assert.Error(t, err)
}

func TestEstablishConnection(t *testing.T) {
	log := logging.NoLog{}
	networkID := uint32(0)
	appVersion := version.NewDefaultVersion("app", 0, 1, 0)
	versionParser := version.NewDefaultParser()

	ip0 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 0,
	}
	id0 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip0.String())))
	ip1 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 1,
	}
	id1 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip1.String())))

	listener0 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller0 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		outbounds: make(map[string]*testListener),
	}
	listener1 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller1 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		outbounds: make(map[string]*testListener),
	}

	caller0.outbounds[ip1.String()] = listener1
	caller1.outbounds[ip0.String()] = listener0

	serverUpgrader := NewIPUpgrader()
	clientUpgrader := NewIPUpgrader()

	vdrs := validators.NewSet()

	var (
		wg0 sync.WaitGroup
		wg1 sync.WaitGroup
	)
	wg0.Add(1)
	wg1.Add(1)

	handler0 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id0) {
				wg0.Done()
			}
		},
	}

	handler1 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				wg1.Done()
			}
		},
	}

	net0 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id0,
		ip0,
		networkID,
		appVersion,
		versionParser,
		listener0,
		caller0,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler0,
	)
	assert.NotNil(t, net0)

	net1 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id1,
		ip1,
		networkID,
		appVersion,
		versionParser,
		listener1,
		caller1,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler1,
	)
	assert.NotNil(t, net1)

	net0.Track(ip1)

	go func() {
		err := net0.Dispatch()
		assert.Error(t, err)
	}()
	go func() {
		err := net1.Dispatch()
		assert.Error(t, err)
	}()

	wg0.Wait()
	wg1.Wait()

	err := net0.Close()
	assert.NoError(t, err)

	err = net1.Close()
	assert.NoError(t, err)
}

func TestDoubleTrack(t *testing.T) {
	log := logging.NoLog{}
	networkID := uint32(0)
	appVersion := version.NewDefaultVersion("app", 0, 1, 0)
	versionParser := version.NewDefaultParser()

	ip0 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 0,
	}
	id0 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip0.String())))
	ip1 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 1,
	}
	id1 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip1.String())))

	listener0 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller0 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		outbounds: make(map[string]*testListener),
	}
	listener1 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller1 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		outbounds: make(map[string]*testListener),
	}

	caller0.outbounds[ip1.String()] = listener1
	caller1.outbounds[ip0.String()] = listener0

	serverUpgrader := NewIPUpgrader()
	clientUpgrader := NewIPUpgrader()

	vdrs := validators.NewSet()

	var (
		wg0 sync.WaitGroup
		wg1 sync.WaitGroup
	)
	wg0.Add(1)
	wg1.Add(1)

	handler0 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id0) {
				wg0.Done()
			}
		},
	}

	handler1 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				wg1.Done()
			}
		},
	}

	net0 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id0,
		ip0,
		networkID,
		appVersion,
		versionParser,
		listener0,
		caller0,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler0,
	)
	assert.NotNil(t, net0)

	net1 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id1,
		ip1,
		networkID,
		appVersion,
		versionParser,
		listener1,
		caller1,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler1,
	)
	assert.NotNil(t, net1)

	net0.Track(ip1)
	net0.Track(ip1)

	go func() {
		err := net0.Dispatch()
		assert.Error(t, err)
	}()
	go func() {
		err := net1.Dispatch()
		assert.Error(t, err)
	}()

	wg0.Wait()
	wg1.Wait()

	err := net0.Close()
	assert.NoError(t, err)

	err = net1.Close()
	assert.NoError(t, err)
}

func TestDoubleClose(t *testing.T) {
	log := logging.NoLog{}
	networkID := uint32(0)
	appVersion := version.NewDefaultVersion("app", 0, 1, 0)
	versionParser := version.NewDefaultParser()

	ip0 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 0,
	}
	id0 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip0.String())))
	ip1 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 1,
	}
	id1 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip1.String())))

	listener0 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller0 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		outbounds: make(map[string]*testListener),
	}
	listener1 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller1 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		outbounds: make(map[string]*testListener),
	}

	caller0.outbounds[ip1.String()] = listener1
	caller1.outbounds[ip0.String()] = listener0

	serverUpgrader := NewIPUpgrader()
	clientUpgrader := NewIPUpgrader()

	vdrs := validators.NewSet()

	var (
		wg0 sync.WaitGroup
		wg1 sync.WaitGroup
	)
	wg0.Add(1)
	wg1.Add(1)

	handler0 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id0) {
				wg0.Done()
			}
		},
	}

	handler1 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				wg1.Done()
			}
		},
	}

	net0 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id0,
		ip0,
		networkID,
		appVersion,
		versionParser,
		listener0,
		caller0,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler0,
	)
	assert.NotNil(t, net0)

	net1 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id1,
		ip1,
		networkID,
		appVersion,
		versionParser,
		listener1,
		caller1,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler1,
	)
	assert.NotNil(t, net1)

	net0.Track(ip1)

	go func() {
		err := net0.Dispatch()
		assert.Error(t, err)
	}()
	go func() {
		err := net1.Dispatch()
		assert.Error(t, err)
	}()

	wg0.Wait()
	wg1.Wait()

	err := net0.Close()
	assert.NoError(t, err)

	err = net1.Close()
	assert.NoError(t, err)

	err = net0.Close()
	assert.NoError(t, err)

	err = net1.Close()
	assert.NoError(t, err)
}

func TestTrackConnected(t *testing.T) {
	log := logging.NoLog{}
	networkID := uint32(0)
	appVersion := version.NewDefaultVersion("app", 0, 1, 0)
	versionParser := version.NewDefaultParser()

	ip0 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 0,
	}
	id0 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip0.String())))
	ip1 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 1,
	}
	id1 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip1.String())))

	listener0 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller0 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		outbounds: make(map[string]*testListener),
	}
	listener1 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller1 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		outbounds: make(map[string]*testListener),
	}

	caller0.outbounds[ip1.String()] = listener1
	caller1.outbounds[ip0.String()] = listener0

	serverUpgrader := NewIPUpgrader()
	clientUpgrader := NewIPUpgrader()

	vdrs := validators.NewSet()

	var (
		wg0 sync.WaitGroup
		wg1 sync.WaitGroup
	)
	wg0.Add(1)
	wg1.Add(1)

	handler0 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id0) {
				wg0.Done()
			}
		},
	}

	handler1 := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				wg1.Done()
			}
		},
	}

	net0 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id0,
		ip0,
		networkID,
		appVersion,
		versionParser,
		listener0,
		caller0,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler0,
	)
	assert.NotNil(t, net0)

	net1 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id1,
		ip1,
		networkID,
		appVersion,
		versionParser,
		listener1,
		caller1,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler1,
	)
	assert.NotNil(t, net1)

	net0.Track(ip1)

	go func() {
		err := net0.Dispatch()
		assert.Error(t, err)
	}()
	go func() {
		err := net1.Dispatch()
		assert.Error(t, err)
	}()

	wg0.Wait()
	wg1.Wait()

	net0.Track(ip1)

	err := net0.Close()
	assert.NoError(t, err)

	err = net1.Close()
	assert.NoError(t, err)
}

func TestTrackConnectedRace(t *testing.T) {
	log := logging.NoLog{}
	networkID := uint32(0)
	appVersion := version.NewDefaultVersion("app", 0, 1, 0)
	versionParser := version.NewDefaultParser()

	ip0 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 0,
	}
	id0 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip0.String())))
	ip1 := utils.IPDesc{
		IP:   net.IPv6loopback,
		Port: 1,
	}
	id1 := ids.NewShortID(hashing.ComputeHash160Array([]byte(ip1.String())))

	listener0 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller0 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 0,
		},
		outbounds: make(map[string]*testListener),
	}
	listener1 := &testListener{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		inbound: make(chan net.Conn, 1<<10),
		closed:  make(chan struct{}),
	}
	caller1 := &testDialer{
		addr: &net.TCPAddr{
			IP:   net.IPv6loopback,
			Port: 1,
		},
		outbounds: make(map[string]*testListener),
	}

	caller0.outbounds[ip1.String()] = listener1
	caller1.outbounds[ip0.String()] = listener0

	serverUpgrader := NewIPUpgrader()
	clientUpgrader := NewIPUpgrader()

	vdrs := validators.NewSet()
	handler := &testHandler{}

	net0 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id0,
		ip0,
		networkID,
		appVersion,
		versionParser,
		listener0,
		caller0,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler,
	)
	assert.NotNil(t, net0)

	net1 := NewDefaultNetwork(
		prometheus.NewRegistry(),
		log,
		id1,
		ip1,
		networkID,
		appVersion,
		versionParser,
		listener1,
		caller1,
		serverUpgrader,
		clientUpgrader,
		vdrs,
		vdrs,
		handler,
	)
	assert.NotNil(t, net1)

	net0.Track(ip1)

	go func() {
		err := net0.Dispatch()
		assert.Error(t, err)
	}()
	go func() {
		err := net1.Dispatch()
		assert.Error(t, err)
	}()

	err := net0.Close()
	assert.NoError(t, err)

	err = net1.Close()
	assert.NoError(t, err)
}

// Test peers repeatedly disconnecting and connecting
// Each reconnection should be with a higher session ID
func TestReconnect(t *testing.T) {
	net0Connected, net0Disconnected, net0, net1Connected, net1Disconnected, net1 := testNetwork(t)

	// Disconnect and reconnect repeatedly
	for i := uint32(0); i < 10; i++ {
		net0Connected.Add(1)
		net1Connected.Add(1)
		net0Disconnected.Add(1)
		net1Disconnected.Add(1)

		// Make sure session IDs match
		peerSessionID0, ok := net0.nextSessionID[id1.Key()]
		if !ok {
			t.Fatal("should have next session ID")
		}
		peerSessionID1, ok := net1.nextSessionID[id0.Key()]
		if !ok {
			t.Fatal("should have next session ID")
		}
		if peerSessionID0 != peerSessionID1 {
			t.Fatal("next session IDs should match")
		}
		if peerSessionID0 != i+1 {
			t.Fatalf("next session ID is %d but should be %d", peerSessionID0, i+1)
		}

		// Cause net1's connection to net0 to close
		err := net1.peers[id0.Key()].conn.Close()
		assert.NoError(t, err)

		// Wait until the nodes disconnect and then reconnect
		net1Disconnected.Wait()
		net0Disconnected.Wait()
		net0Connected.Wait()
		net1Connected.Wait()
		// The two peers are connected

		_, connected := net0.peers[id1.Key()]
		if !connected {
			t.Fatal("should be connected")
		}
		_, connected = net1.peers[id0.Key()]
		if !connected {
			t.Fatal("should be connected")
		}
	}

	// Calling Close() below decrements these waitgroups...add 1 to each
	// to avoid the waitgroup falling below 0
	net0Disconnected.Add(1)
	net1Disconnected.Add(1)
	err := net0.Close()
	assert.NoError(t, err)
	err = net1.Close()
	assert.NoError(t, err)
}
func TestReconnectHigherSessionID(t *testing.T) {
	net0Connected, net0Disconnected, net0, _, net1Disconnected, net1 := testNetwork(t)

	var (
		cloneConnected    sync.WaitGroup
		cloneDisconnected sync.WaitGroup
	)

	cloneHandler := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				cloneConnected.Done()
			}
		},
		disconnected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				cloneDisconnected.Done()
			}
		},
	}

	net1Clone := NewDefaultNetwork(
		prometheus.NewRegistry(),
		logging.NoLog{},
		id1,
		ip1,
		testNetworkID,
		testAppVersion,
		testVersionParser,
		net1.listener,
		net1.dialer,
		testUpgrader(),
		testUpgrader(),
		testVdrs,
		testVdrs,
		cloneHandler,
	)
	assert.NotNil(t, net1)
	net1Clone.(*network).nextSessionID[id0.Key()] = 100

	// net0 should disconnect from net1 and connect to net1Clone
	net0Disconnected.Add(1)
	net0Connected.Add(1)
	// net1Clone should connect to net0
	cloneConnected.Add(1)

	net1Clone.Track(ip0)

	// net0 should disconnect from net1 and connect to net1Clone
	net0Disconnected.Wait()
	net0Connected.Wait()
	// net1Clone should connect to net0
	cloneConnected.Wait()

	cloneSessionID := net1Clone.(*network).nextSessionID[id0.Key()]
	if cloneSessionID != 101 {
		t.Fatalf("next session ID should be 101 but is %d", cloneSessionID)
	}
	net0SessionID := net0.nextSessionID[id1.Key()]
	if net0SessionID != 101 {
		t.Fatalf("next session ID should be 101 but is %d", net0SessionID)
	}

	// Calling Close() below decrements these waitgroups...add 1 to each
	// to avoid the waitgroup falling below 0
	net0Disconnected.Add(1)
	net1Disconnected.Add(1)
	cloneDisconnected.Add(1)
	err := net0.Close()
	assert.NoError(t, err)
	err = net1.Close()
	assert.NoError(t, err)
	err = net1Clone.Close()
	assert.NoError(t, err)
}

// Test that ensures a peer only drops an existing connection
// to a peer for a new one if the incoming session ID
// id is 0 or greater than the current one.
// For this test we:
// 1) Connect peer0 and peer1
// 2) Make a "clone" of peer1 (same node ID)
// 3) Tell the clone to connect with peer0 with a request ID <= the current one
// 4) Make sure peer0 ignores the new connection from the "clone"
func TestReconnectLowerSessionID(t *testing.T) {
	_, net0Disconnected, net0, _, net1Disconnected, net1 := testNetwork(t)

	net0.stateLock.Lock()
	net1.stateLock.Lock()
	// Set the session IDs to 100
	net0.nextSessionID[id1.Key()] = 100
	net1.nextSessionID[id0.Key()] = 100
	net0.stateLock.Unlock()
	net1.stateLock.Unlock()

	var (
		cloneConnected    sync.WaitGroup
		cloneDisconnected sync.WaitGroup
	)

	cloneHandler := &testHandler{
		connected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				cloneConnected.Done()
			}
		},
		disconnected: func(id ids.ShortID) {
			if !id.Equals(id1) {
				cloneDisconnected.Done()
			}
		},
	}
	// "clone" of net1 (same node ID)
	clone := NewDefaultNetwork(
		prometheus.NewRegistry(),
		net1.log,
		id1,
		ip1,
		testNetworkID,
		testAppVersion,
		testVersionParser,
		net1.listener,
		net1.dialer,
		testUpgrader(),
		testUpgrader(),
		testVdrs,
		testVdrs,
		cloneHandler,
	)

	assert.NotNil(t, clone)
	clone.(*network).nextSessionID[id0.Key()] = 99 // Set session ID with peer id0 to 5
	clone.Track(ip0)
	// Sleep for enough time for a peer to reject this connection attempt
	// TODO: there's probably a better way to check that this connection is rejected
	time.Sleep(100 * time.Millisecond)

	_, isConnected := clone.(*network).peers[id0.Key()]
	if isConnected {
		t.Fatalf("should not be connected")
	}

	// Calling Close() below decrements these waitgroups...add 1 to each
	// to avoid the waitgroup falling below 0
	net0Disconnected.Add(1)
	net1Disconnected.Add(1)
	cloneDisconnected.Add(1)
	err := net0.Close()
	assert.NoError(t, err)
	err = net1.Close()
	assert.NoError(t, err)
	err = clone.Close()
	assert.NoError(t, err)
}
