// package nattywad implements NAT traversal using go-natty and waddell.
package nattywad

import (
	"encoding/binary"
	"net"
	"sync"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/waddell"
)

const (
	ServerReady = "ServerReady"
	Timeout     = 30 * time.Second
)

var (
	log = golog.LoggerFor("nattywad")

	maxWaddellMessageSize = 4096 + waddell.WADDELL_OVERHEAD

	endianness = binary.LittleEndian
)

// FiveTupleCallbackClient is a function that gets invoked when a client NAT
// traversal results in a UDP five tuple.
type FiveTupleCallbackClient func(local *net.UDPAddr, remote *net.UDPAddr)

// FiveTupleCallbackServer is a function that gets invoked when a server NAT
// traversal results in a UDP five tuple. The function allows the consumer of
// nattywad to bind to the provided local and remote addresses and start
// whatever processing it needs to. FiveTupleCallback should return true to
// indicate that the server is bound and ready, which will cause nattywad to
// emit a ServerReady message. Only once this has happened will the client on
// the other side of the NAT traversal actually get a five tuple through its
// own callback.
type FiveTupleCallbackServer func(local *net.UDPAddr, remote *net.UDPAddr) bool

type message []byte

func (msg message) setTraversalId(id uint32) {
	endianness.PutUint32(msg[:4], id)
}

func (msg message) getTraversalId() uint32 {
	return endianness.Uint32(msg[:4])
}

func (msg message) getData() []byte {
	return msg[4:]
}

// waddellConn represents a connection to waddell. It automatically redials if
// there is a problem reading or writing.  As implemented, it provides
// half-duplex communication even though waddell supports full-duplex.  Given
// the low bandwidth and weak latency requirements of signaling traffic, this is
// unlikely to be a problem.
type waddellConn struct {
	dial      func() (net.Conn, error)
	client    *waddell.Client
	conn      net.Conn
	connMutex sync.RWMutex
	dialErr   error
}

// newWaddellConn establishes a new waddellConn that uses the provided dial
// function to connect to waddell when it needs to.
func newWaddellConn(dial func() (net.Conn, error)) (wc *waddellConn, err error) {
	wc = &waddellConn{
		dial: dial,
	}
	err = wc.connect()
	return
}

func (wc *waddellConn) send(peerId waddell.PeerId, sessionId uint32, msgOut string) (err error) {
	log.Tracef("Sending message %s to peer %s in session %d", msgOut, peerId, sessionId)
	client, dialErr := wc.getClient()
	if dialErr != nil {
		err = dialErr
		return
	}
	err = client.SendPieces(peerId, idToBytes(sessionId), []byte(msgOut))
	if err != nil {
		wc.connError(client, err)
	}
	return
}

func (wc *waddellConn) receive() (msg message, from waddell.PeerId, err error) {
	log.Trace("Receiving")
	client, dialErr := wc.getClient()
	if dialErr != nil {
		err = dialErr
		return
	}
	b := make([]byte, maxWaddellMessageSize)
	var wm *waddell.Message
	wm, err = wc.client.Receive(b)
	if err != nil {
		wc.connError(client, err)
		log.Tracef("Error receiving: %s", err)
		return
	}
	from = wm.From
	msg = message(wm.Body)
	log.Tracef("Received %s from %s", msg.getData(), from)
	return
}

func (wc *waddellConn) getClient() (*waddell.Client, error) {
	wc.connMutex.RLock()
	defer wc.connMutex.RUnlock()
	return wc.client, wc.dialErr
}

func (wc *waddellConn) connError(client *waddell.Client, err error) {
	wc.connMutex.Lock()
	log.Tracef("Error on waddell connection: %s", err)
	if client == wc.client {
		// The current client is in error, redial
		go func() {
			log.Tracef("Redialing waddell")
			wc.dialErr = wc.connect()
			wc.connMutex.Unlock()
		}()
	} else {
		wc.connMutex.Unlock()
	}
}

func (wc *waddellConn) connect() (err error) {
	log.Trace("Connecting to waddell")
	wc.conn, err = wc.dial()
	if err != nil {
		return
	}
	wc.client, err = waddell.Connect(wc.conn)
	if err == nil {
		log.Debugf("Connected to Waddell!! Id is: %s", wc.client.ID())
	} else {
		log.Debugf("Unable to connect waddell client: %s", err)
	}
	return
}

func (wc *waddellConn) close() error {
	return wc.conn.Close()
}

func idToBytes(id uint32) []byte {
	b := make([]byte, 4)
	endianness.PutUint32(b[:4], id)
	return b
}
