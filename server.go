package nattywad

import (
	"net"
	"sync"
	"time"

	"github.com/getlantern/go-natty/natty"
	log "github.com/getlantern/golog"
	"github.com/getlantern/waddell"
)

// Server is a server that answers NAT traversal requests received via waddell.
// When a NAT traversal results in a 5-tuple, the OnFiveTuple callback is
// called in its own goroutine.
type Server struct {
	// OnFiveTuple: a callback that's invoked once a five tuple has been
	// obtained. Must be specified in order for Client to work.
	OnFiveTuple FiveTupleCallbackServer

	waddellAddr string
	worker      *serverWorker
	cfgMutex    sync.Mutex
}

// Configure (re)configures the server to communicate through the given
// waddellAddr. Anytime that waddellAddr changes, Server will connect to the new
// waddell instance and start accepting offers from it. Whenever a waddell
// connection is established, Server will log a message to stderr like the below
// in order to allow consumers of flashlight to find out the peer id that's
// been assigned by waddell:
//
//   Connected to Waddell!! Id is: 4fb42b23-78d3-4185-b1d7-46b7d4eb9167
//
func (server *Server) Configure(waddellAddr string) {
	server.cfgMutex.Lock()
	defer server.cfgMutex.Unlock()

	if waddellAddr != server.waddellAddr {
		log.Debugf("Waddell address changed")
		if server.worker != nil {
			server.worker.stop()
		}

		server.waddellAddr = waddellAddr
		if server.waddellAddr != "" {
			wc, err := newWaddellConn(func() (net.Conn, error) {
				return net.DialTimeout("tcp", waddellAddr, 20*time.Second)
			})
			if err != nil {
				log.Errorf("Unable to connect to waddell: %s", err)
			} else {
				server.worker = startServerWorker(wc, server.OnFiveTuple)
			}
		}
	}
}

// serverWorker encapsulates the work that's done to accept offers on a waddell
// connection. Every new waddell connection gets its own serverWorker in order
// to make sure that we don't mix traversals between server connections.
type serverWorker struct {
	wc          *waddellConn
	onFiveTuple FiveTupleCallbackServer
	stopCh      chan bool
	peers       map[waddell.PeerId]*peer
	peersMutex  sync.Mutex
}

func startServerWorker(wc *waddellConn, onFiveTuple FiveTupleCallbackServer) *serverWorker {
	worker := &serverWorker{
		wc:          wc,
		onFiveTuple: onFiveTuple,
		stopCh:      make(chan bool),
		peers:       make(map[waddell.PeerId]*peer),
	}
	go worker.receiveMessages()
	return worker
}

func (w *serverWorker) stop() {
	w.stopCh <- true
}

func (w *serverWorker) receiveMessages() {
	defer func() {
		w.wc.close()
	}()

	for {
		select {
		case <-w.stopCh:
			return
		default:
			msg, from, err := w.wc.receive()
			if err != nil {
				log.Errorf("Error receiving next message from waddell: %s", err)
				continue
			}
			w.processMessage(msg, from)
		}
	}
}

func (w *serverWorker) processMessage(msg message, from waddell.PeerId) {
	w.peersMutex.Lock()
	defer w.peersMutex.Unlock()

	p := w.peers[from]
	if p == nil {
		p = &peer{
			id:          from,
			wc:          w.wc,
			traversals:  make(map[uint32]*natty.Traversal),
			onFiveTuple: w.onFiveTuple,
		}
		w.peers[from] = p
	}
	p.answer(msg)
}

type peer struct {
	id              waddell.PeerId
	wc              *waddellConn
	onFiveTuple     FiveTupleCallbackServer
	traversals      map[uint32]*natty.Traversal
	traversalsMutex sync.Mutex
}

func (p *peer) answer(msg message) {
	p.traversalsMutex.Lock()
	defer p.traversalsMutex.Unlock()
	traversalId := msg.getTraversalId()
	t := p.traversals[traversalId]
	if t == nil {
		// Set up a new Natty traversal
		t = natty.Answer(nil)
		go func() {
			// Send
			for {
				msgOut, done := t.NextMsgOut()
				if done {
					return
				}
				p.wc.send(p.id, traversalId, msgOut)
			}
		}()

		go func() {
			// Receive
			defer func() {
				p.traversalsMutex.Lock()
				defer p.traversalsMutex.Unlock()
				delete(p.traversals, traversalId)
				t.Close()
			}()

			ft, err := t.FiveTupleTimeout(Timeout)
			if err != nil {
				log.Debugf("Unable to answer traversal %d: %s", traversalId, err)
				return
			}

			local, remote, err := ft.UDPAddrs()
			if err != nil {
				log.Errorf("Unable to get UDP addresses for FiveTuple: %s", err)
				return
			}

			if p.onFiveTuple(local, remote) {
				// Server is ready, notify client
				p.wc.send(p.id, traversalId, ServerReady)
			}
		}()
		p.traversals[traversalId] = t
	}
	t.MsgIn(string(msg.getData()))
}
