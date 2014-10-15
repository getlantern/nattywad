package nattywad

import (
	"math/rand"
	"net"
	"sync"

	"github.com/getlantern/go-natty/natty"
	log "github.com/getlantern/golog"
	"github.com/getlantern/waddell"
)

// ServerPeer identifies a server for NAT traversal
type ServerPeer struct {
	ID          string
	WaddellAddr string
}

func (p *ServerPeer) CompositeID() string {
	return p.WaddellAddr + "|" + p.ID
}

func (p *ServerPeer) String() string {
	return p.CompositeID()
}

// Client is a client that initiates NAT traversals to one or more configured
// servers. When a NAT traversal results in a 5-tuple, the OnFiveTuple callback
// is called.
type Client struct {
	// DialWaddell: a function that dials the waddell server at the given
	// address. Must be specified in order for Client to work.
	DialWaddell func(addr string) (net.Conn, error)

	// OnFiveTuple: a callback that's invoked once a five tuple has been
	// obtained. Must be specified in order for Client to work.
	OnFiveTuple FiveTupleCallbackClient

	serverPeers  map[string]*ServerPeer
	workers      map[uint32]*clientWorker
	workersMutex sync.RWMutex
	waddellConns map[string]*waddellConn
	cfgMutex     sync.Mutex
}

// Configure (re)configures this Client to communicate with the given list of
// server peers. Anytime that the list is found to contain a new peer, a NAT
// traversal is attempted to that peer.
func (c *Client) Configure(serverPeers []*ServerPeer) {
	c.cfgMutex.Lock()
	defer c.cfgMutex.Unlock()

	log.Debugf("Configuring nat traversal client with %d server peers", len(serverPeers))

	// Lazily initialize data structures
	if c.serverPeers == nil {
		c.serverPeers = make(map[string]*ServerPeer)
		c.waddellConns = make(map[string]*waddellConn)
		c.workers = make(map[uint32]*clientWorker)
	}

	priorServerPeers := c.serverPeers
	c.serverPeers = make(map[string]*ServerPeer)

	for _, peer := range serverPeers {
		cid := peer.CompositeID()

		if priorServerPeers[cid] == nil {
			// Either we have a new server, or the address changed, try to
			// traverse
			peerId, err := waddell.PeerIdFromString(peer.ID)
			if err != nil {
				log.Errorf("Unable to parse PeerID for server peer %s: %s",
					peer.ID, err)
				continue
			}
			c.offer(peer.WaddellAddr, peerId)
		}

		// Keep track of new peer
		c.serverPeers[cid] = peer
	}
}

func (c *Client) offer(waddellAddr string, peerId waddell.PeerId) {
	wc := c.waddellConns[waddellAddr]
	if wc == nil {
		/* new waddell server--open connection to it */
		var err error
		wc, err = newWaddellConn(func() (net.Conn, error) {
			return c.DialWaddell(waddellAddr)
		})
		if err != nil {
			log.Errorf("Unable to connect to waddell: %s", err)
			return
		}
		c.waddellConns[waddellAddr] = wc
		go c.receiveMessages(wc)
	}

	w := &clientWorker{
		wc:          wc,
		peerId:      peerId,
		onFiveTuple: c.OnFiveTuple,
		traversalId: uint32(rand.Int31()),
		serverReady: make(chan bool, 10), // make this buffered to prevent deadlocks
	}
	c.addWorker(w)
	go w.run()
}

func (c *Client) receiveMessages(wc *waddellConn) {
	for {
		msg, _, err := wc.receive()
		if err != nil {
			log.Errorf("Unable to receive next message from waddell: %s", err)
			return
		}
		w := c.getWorker(msg.getTraversalId())
		if w == nil {
			log.Debugf("Got message for unknown traversal %d, skipping", msg.getTraversalId())
			continue
		}
		w.messageReceived(msg)
	}
}

func (c *Client) addWorker(w *clientWorker) {
	c.workersMutex.Lock()
	defer c.workersMutex.Unlock()
	c.workers[w.traversalId] = w
}

func (c *Client) getWorker(traversalId uint32) *clientWorker {
	c.workersMutex.RLock()
	defer c.workersMutex.RUnlock()
	return c.workers[traversalId]
}

func (c *Client) removeWorker(w *clientWorker) {
	c.workersMutex.Lock()
	defer c.workersMutex.Unlock()
	delete(c.workers, w.traversalId)
}

// clientWorker encapsulates the work done by the client for a single NAT
// traversal.
type clientWorker struct {
	wc          *waddellConn
	peerId      waddell.PeerId
	onFiveTuple FiveTupleCallbackClient
	traversalId uint32
	traversal   *natty.Traversal
	serverReady chan bool
}

func (w *clientWorker) run() {
	w.traversal = natty.Offer(nil)
	defer w.traversal.Close()

	go w.sendMessages()

	ft, err := w.traversal.FiveTupleTimeout(Timeout)
	if err != nil {
		log.Errorf("Traversal to %s failed: %s", w.peerId, err)
		return
	}
	if <-w.serverReady {
		local, remote, err := ft.UDPAddrs()
		if err != nil {
			log.Errorf("Unable to get UDP addresses for FiveTuple: %s", err)
			return
		}
		w.onFiveTuple(local, remote)
	}
}

func (w *clientWorker) sendMessages() {
	for {
		msgOut, done := w.traversal.NextMsgOut()
		if done {
			return
		}
		w.wc.send(w.peerId, w.traversalId, msgOut)
	}
}

func (w *clientWorker) messageReceived(msg message) {
	msgString := string(msg.getData())
	if msgString == ServerReady {
		// Server's ready!
		w.serverReady <- true
	} else {
		w.traversal.MsgIn(msgString)
	}
}
