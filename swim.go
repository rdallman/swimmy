package swimmy

import (
	"container/heap"
	"crypto/rand"
	"encoding/binary"
	"math"
	"math/big"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/inconshreveable/log15.v2"
)

type Config struct {
	// Necessary paremeters
	Host  string // TODO get this dynamically
	Port  uint16 // TODO pick a default
	Seeds []*net.UDPAddr

	// Optional custom parameters w/ sane defaults
	GossipInterval time.Duration // default: 200ms
	FailInterval   time.Duration // default: 1s
	Timeout        time.Duration // default: 10ms
	MaxBufferLen   int           // max piggybacks to send on one request. default: 6
	K              int           // number of people to ping-req on failed ack. default: 1 TODO 2?
	Lambda         int           // λ log n times to piggyback an event. default: 3

	// Listen for events and make decisions yourself. Make sure
	// to pull from this channel reasonably fast.
	Listeners []chan Event
}

type Event struct {
	Who  *net.UDPAddr
	What EventType
}

type peer struct {
	Who   *net.UDPAddr
	State EventType
	Time  uint32
}

const (
	Alive EventType = iota
	Suspect
	Confirm
	// note: these are dual purpose with node state

	alive   = "alive"
	suspect = "suspect"
	confirm = "confirm"
	invalid = "invalid"
)

type EventType byte

func (e EventType) String() string {
	switch e {
	case Alive:
		return alive
	case Suspect:
		return suspect
	case Confirm:
		return confirm
	}
	return invalid
}

// Cliques gossip about who their real friends are. like high school all over again.
type Clique struct {
	conn           *net.UDPConn
	done           chan struct{}
	me             *net.UDPAddr
	gossipInterval time.Duration
	failInterval   time.Duration
	timeout        time.Duration
	maxBuf         int
	k, lambda      int
	listeners      []chan Event

	pals []*net.UDPAddr   // Alive U Suspect, shuffled each round
	mems map[string]*peer // Alive U Suspect U Confirm

	pigBuffer pigs // piggy heap

	acks    map[string]time.Time
	inbox   chan unread
	outbox  chan draft
	propose chan peer

	// these act like locks for safe reads of peer list in user facing operations.
	// read from  read is like mu.Lock() and send to release is like mu.Unlock()
	read, release chan struct{}

	bufPool *sync.Pool // TODO fix this

	time uint32
}

func New(conf *Config) (*Clique, error) {
	me, err := net.ResolveUDPAddr("udp", conf.Host+":"+strconv.Itoa(int(conf.Port)))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", me)
	if err != nil {
		return nil, err
	}
	if conf.GossipInterval < 1 {
		conf.GossipInterval = 200 * time.Millisecond
	}
	if conf.FailInterval < 1 {
		conf.FailInterval = 1 * time.Second
	}
	if conf.Timeout < 1 {
		conf.Timeout = 10 * time.Millisecond
	}
	if conf.K < 1 {
		conf.K = 1
	}
	if conf.Lambda < 1 {
		conf.Lambda = 3
	}
	if conf.MaxBufferLen < 1 {
		conf.MaxBufferLen = 6
	}
	c := &Clique{
		me:             me,
		conn:           conn,
		done:           make(chan struct{}),
		mems:           make(map[string]*peer),
		listeners:      conf.Listeners,
		timeout:        conf.Timeout,
		gossipInterval: conf.GossipInterval,
		failInterval:   conf.FailInterval,
		acks:           make(map[string]time.Time), // map[node]pingTime
		k:              conf.K,
		lambda:         conf.Lambda,
		maxBuf:         conf.MaxBufferLen,

		// event channels, all synchronized.
		// we must take care to buffer so we don't block ourselves.
		// TODO do we really need 1024 tho? really? really really?
		inbox:   make(chan unread, 1024),
		outbox:  make(chan draft, 1024),
		propose: make(chan peer, 1024),

		// simulate mutexes, for users to safely read into our state.
		read:    make(chan struct{}),
		release: make(chan struct{}),

		// share a pool of buffers
		bufPool: &sync.Pool{New: func() interface{} {
			return make([]byte, maxPacketSize)
		}},
	}
	heap.Init(&c.pigBuffer)
	c.mems[me.String()] = &peer{Who: me, State: Alive, Time: 0}
	// tell a few guys about ourselves
	c.upsertPiggy(peer{Who: c.me, State: Alive, Time: c.time})
	go c.listen()
	go c.Seed(conf.Seeds) // TODO make this explicit for caller?
	go c.swim()
	return c, nil
}

func (c *Clique) Me() net.Addr { return c.me }

// Alive returns a copied list of peers that are currently alive.
//
// below lay an idea. not sure if the api is very friendly, it's confusing.
// To attempt to wait for membership to stabilize, let safe=true
// and we won't return until our incoming and outgoing buffers are flushed.
func (c *Clique) Alive(safe bool) []net.Addr {
	lst := []net.Addr{c.me}
	select {
	case <-c.read:
	case <-c.done:
		return nil // this is confusing, but shouldn't be doing ops anyway
	}
	for _, p := range c.pals {
		if c.mems[p.String()].State == Alive {
			lst = append(lst, p)
		}
	}
	c.release <- struct{}{}
	return lst
}

// commit suicide
func (c *Clique) Die() {
	c.conn.SetReadDeadline(time.Now().Add(-1 * time.Second)) // further reads we'd say we're alive, shut up

	select {
	case <-c.read:
	case <-c.done:
		return // means this has been called already
	}
	c.pigBuffer = []pig{pig{e: peer{Who: c.me, State: Confirm, Time: c.time}}}
	times := c.λlogn() // this seems sufficient for other things.. TODO tell everybody?
	drafts := make([]draft, times)
	for i := range drafts {
		drafts[i] = draft{What: ping, Who: c.randPeer(), Origin: c.me} // this only works b/c buffered
	}
	c.release <- struct{}{}

	for _, d := range drafts {
		c.outbox <- d
	}

	log15.Debug("yo dawg")
	close(c.done)
	c.conn.Close()
}

func (c *Clique) λlogn() int { return int(float64(c.lambda) * math.Log(float64(len(c.pals)))) }

func (c *Clique) Kill(who *net.UDPAddr) {
	c.time++ // this doesn't matter, since any Confirm(i) override any previous state
	c.propose <- peer{Who: who, Time: c.time, State: Confirm}
}

// TODO this ~defeats the purpose. should we pick some arbitrary N? test.
func (c *Clique) Seed(peers []*net.UDPAddr) {
	for _, addr := range peers {
		if addr.String() == c.me.String() {
			continue
		} else if len(c.pals) > 0 {
			break // go until we get a JOINACK
		}

		c.outbox <- draft{What: join, Who: addr, Origin: c.me}
		log15.Debug("joining", "a", addr, "me", c.me)
	}
}

// we round robin through our member set over the gossip intervals.
// every ping we do, adds an entry to the ack table.
// if we don't see a ping in the timeout interval, send K ping-req.
// if we don't see a ping in the gossip interval, mark it suspect.
// if we don't see a ping in the fail interval, mark confirm.
//
// each of our packets are one of { ping, ack, ping-req }.
// piggybacked on to each could be up to a configurable number of events that have
// happened recently. these events are stored in a buffer until
// we have gossipped them λ log n times, at which point they're removed.
//
// a node may add a new event to the chain to gossip when:
// Alive   -> Suspect:     no ack is received in gossip interval
// Suspect -> Confirm::    no ack is received in failure interval
// *       -> Alive:       receive from a node not in alive state
//
// in addition to new events, an event that is seen piggy backed on any received
// messages, will be checked against the current buffer for any disparities,
// such that:
//
// Alive   overrides { Suspect }
// Suspect overrides { Alive }
// Confirm overrides { Suspect, Alive }
//
// any message that does not meet the above criteria but is not already
// in the buffer, will be added to the buffer (it's index uncertain).
//
// we define the packet such that:
//  [ 0     0 1 2 3 4 : 5 6  [ 7 8 9 10 : 11 12    | 13 ] ...  [ TODO tags ] ]
//    type  dst: ip : port    piggyback: ip : port  type
//
// where src is the origination of the message, such that ping-req and it's succeeding
// messages will have dst != from. A full example of a ping-req sequence below.
//
//  TODO reason about the below after sleeping
//  A -> B Ping(To: B, From: A, Origin: A)
// A Timeout(B)
// A -> C PingReq(To: C, From: A, Origin: B)
// C -> B Ping(To: B, From: C, Origin: A)
// B -> C Ack(To: C, From: B, Origin: A)
// C -> A Ack(To: A, From: C, Origin: B)
//
// TODO generation bits, tags

type reqT byte

const (
	ping reqT = iota
	pingReq
	ack
	join
	joinAck

	piggySize     = net.IPv4len + 2 + 1 // sizeof(uint16) + TODO gen bits
	maxPacketSize = 508                 // IPv4 minimum maximum reassembly buffer size [576] - max IP Header [60] - UDP Header [8] -- https://tools.ietf.org/html/rfc1122
)

type unread struct {
	From *net.UDPAddr
	Body []byte
}

type draft struct {
	Who    *net.UDPAddr
	Origin *net.UDPAddr
	What   reqT
}

// Implements sort.Interface and heap.Interface
type pigs []pig

type pig struct {
	e peer
	c int // number of times we've gossipped this event
}

func (p pigs) Len() int { return len(p) }

func (p pigs) Less(i, j int) bool { return p[i].c < p[j].c }

func (p pigs) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (p *pigs) Push(x interface{}) { *p = append(*p, x.(pig)) }

func (p *pigs) Pop() interface{} { old := *p; pe := old[len(old)-1]; *p = old[:len(old)-1]; return pe }

// This function is basically it. We can only allow a single
// writer and a single reader at a time on the conn,
// but this func only allows one of either. we could do more, but why?
//
// the goal of this is to synchronize reads and writes to:
//   pals
//   members
//   piggyBuffer
//
// since each is potentially a writer and a reader of 1 or more
// and since traffic should be low, we prefer less mutexes and
// easier to read codes.
//
// TODO should be easy to deadlock inbox/outbox sends if buffer fills; fix
func (c *Clique) swim() {
	var i uint // round robin speeds things up
	var timeout <-chan time.Time
	for {
		select {
		case <-time.Tick(c.gossipInterval):
			c.ping(i)
			i++
			timeout = time.After(c.timeout)
		case <-timeout:
			c.reap()
		case in := <-c.inbox:
			c.readAndReply(in)
		case out := <-c.outbox:
			c.send(out)
		case prop := <-c.propose:
			c.ponder(prop)
		case c.read <- struct{}{}:
			select {
			case <-c.release: // wait, so caller can read safely
			case <-c.done:
			}
		case <-c.done:
			return
		}
	}
}

// listen and send each request to the inbox
func (c *Clique) listen() {
	for {
		select {
		case <-c.done:
			return
		default:
			p := c.bufPool.Get().([]byte)
			n, addr, err := c.conn.ReadFromUDP(p)
			if err != nil {
				log15.Warn("gossip couldn't read real good", "err", err, "addr", addr)
				continue
			}
			c.inbox <- unread{From: addr, Body: p[:n]}
		}
	}
}

// round robin through our peer set, sending PING and adding an ack to await
func (c *Clique) ping(i uint) {
	log15.Debug("chans", "in", len(c.inbox), "out", len(c.outbox), "props", len(c.propose), "mems", len(c.mems), "buf", len(c.pigBuffer))
	if len(c.pals) < 1 {
		return // nobody to ping ¯\_(ツ)_/¯
	}
	peer := c.pals[i%uint(len(c.pals))] // round robin
	c.outbox <- draft{What: ping, Who: peer, Origin: c.me}
	if _, ok := c.acks[peer.String()]; !ok {
		// don't overwrite any old acks we're awaiting
		c.acks[peer.String()] = time.Now()
	}
	if i%uint(len(c.pals)) == 0 { // shuffle each round
		c.shufflePals()
	}
}

func (c *Clique) shufflePals() {
	// fisher & yates' idea, not mine
	for i := len(c.pals) - 1; i >= 1; i-- {
		r, _ := rand.Int(rand.Reader, big.NewInt(int64(i)))
		j := r.Int64()
		c.pals[i], c.pals[j] = c.pals[j], c.pals[i]
	}
}

// periodically sweep the acks and escalate the state
// of any peers we're having trouble communicating with.
func (c *Clique) reap() {
	for addr, t := range c.acks {
		elapsed := time.Now().Sub(t)
		who, _ := net.ResolveUDPAddr("udp", addr) // no err, we made it

		if elapsed > c.failInterval {
			log15.Warn("dead!", "who", addr, "me", c.me, "time", c.mems[addr].Time)
			c.propose <- peer{Who: who, State: Confirm, Time: c.mems[addr].Time}

			delete(c.acks, who.String())

		} else if elapsed > c.gossipInterval {
			log15.Warn("suspect!", "who", addr, "me", c.me, "time", c.mems[addr].Time)
			c.propose <- peer{Who: who, State: Suspect, Time: c.mems[addr].Time}

		} else if elapsed > c.timeout {
			log15.Warn("timeout!", "who", addr, "me", c.me)
			// no ack, send K random ping-req
			for i := 0; i < c.k && len(c.pals)-i > i; i++ {
				peer := c.randPeer()
				if peer.String() == addr {
					i-- // try somebody else, we just tried to ping this guy
					continue
				}
				c.outbox <- draft{What: pingReq, Who: peer, Origin: c.me}
			}
		}
	}
}

func (c *Clique) randPeer() *net.UDPAddr {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(c.pals))))
	return c.pals[n.Int64()]
}

// see what kind of mail we have, and reply accordingly.
func (c *Clique) readAndReply(in unread) {
	from := in.From
	b := in.Body
	if len(b) < 7 || from.String() == c.me.String() {
		return // don't try to process bad packets / loopbacks
	}
	origin := parseIPPort(b[1:7])

	delete(c.acks, from.String()) // if we hear from someone, they ain't gone

	// for people we've never heard from, add them
	if c.addToPals(from, 0) {
		c.propose <- peer{Who: from, State: Alive, Time: 0}
	}

	switch reqT(b[0]) {
	case join:
		log15.Debug("join", "c", atomic.LoadUint32(&c.time), "me", c.me, "from", from, "wtf", origin)
		c.outbox <- draft{What: joinAck, Who: from, Origin: from}
	case joinAck:
		log15.Debug("joinAck", "c", atomic.LoadUint32(&c.time), "me", c.me, "from", from, "wtf", origin)
		for i := 7; i+10 < len(b); {
			log15.Info("addin to pals", "i", i, "p", parseIPPort(b[i:i+6]), "time", binary.LittleEndian.Uint32(b[i+6:i+10]))
			c.addToPals(parseIPPort(b[i:i+6]), binary.LittleEndian.Uint32(b[i+6:i+10])) // TODO make type for time
			i += 10
		}
		return
	case ping:
		log15.Debug("ping", "c", atomic.LoadUint32(&c.time), "me", c.me, "from", from, "wtf", origin)

		if origin.String() == from.String() {
			origin = c.me
		} // else, this means we were suspect in timeout
		c.outbox <- draft{What: ack, Who: from, Origin: origin}
	case pingReq:
		log15.Debug("pingReq", "c", atomic.LoadUint32(&c.time), "me", c.me, "from", from, "wtf", origin)

		// ping the guy who 'from' is suspicious of
		c.outbox <- draft{What: ping, Who: origin, Origin: from}
	case ack:
		log15.Debug("ack", "c", atomic.LoadUint32(&c.time), "me", c.me, "from", from, "wtf", origin)

		if origin.String() != from.String() { // proxying an ack back
			delete(c.acks, origin.String())
			c.outbox <- draft{What: ack, Who: origin, Origin: from}
		}
	}
	c.processPiggies(b[7:])
}

func (c *Clique) addToPals(guy *net.UDPAddr, time uint32) bool {
	_, ok := c.mems[guy.String()]
	if !ok {
		c.mems[guy.String()] = &peer{Who: guy, State: Alive, Time: time}
		c.pals = append(c.pals, guy)
		return true
	}
	return false
}

func (c *Clique) processPiggies(raw []byte) {
	// [ 0 1 2 3 : 4 5    6    7 8 9 10 ]
	//   ip       port  state   time
	for len(raw) > 10 {
		who := parseIPPort(raw[:6])
		wat := EventType(byte(raw[6]))
		when := binary.LittleEndian.Uint32(raw[7:11])
		raw = raw[11:]

		log15.Debug("got a pig", "who", who, "wat", wat, "when", when)
		c.propose <- peer{Who: who, State: wat, Time: when}
	}
}

func parseIPPort(b []byte) *net.UDPAddr {
	return &net.UDPAddr{
		IP:   net.IPv4(b[0], b[1], b[2], b[3]),
		Port: int(binary.LittleEndian.Uint16(b[4:6])),
	}
}

// TODO little funcs reed
// [ 0     1 2 3 4 : 5 6   [ 1 2 3 4 : 5 6   7    8 9 10 11 ] ... ]
// type   to: host:port     host:port      event time
func (c *Clique) send(out draft) {
	buf := c.bufPool.Get().([]byte)
	buf[0] = byte(out.What)
	i := 1
	i += copy(buf[i:], out.Origin.IP.To4())
	binary.LittleEndian.PutUint16(buf[i:], uint16(out.Origin.Port))
	i += 2

	if out.What == joinAck {
		// send out as many of our peers as we can in one packet to get them up to speed; omit other gossip
		for j := 0; j < maxPacketSize/(4+2+4) && j < len(c.pals); j++ { // TODO const for IP + port
			p := c.pals[j]
			i += copy(buf[i:], p.IP.To4())
			binary.LittleEndian.PutUint16(buf[i:], uint16(p.Port))
			i += 2
			binary.LittleEndian.PutUint32(buf[i:], c.mems[p.String()].Time) // TODO MarshalBinary
			i += 4
		}
	} else {
		// every other type of request, gossip as many things as we're allowed
		for j := 0; j < len(c.pigBuffer) && j < c.maxBuf; j++ {
			p := heap.Pop(&c.pigBuffer).(pig)

			i += copy(buf[i:], p.e.Who.IP.To4())
			binary.LittleEndian.PutUint16(buf[i:], uint16(p.e.Who.Port))
			i += 2
			buf[i] = byte(p.e.State)
			i++
			binary.LittleEndian.PutUint32(buf[i:], p.e.Time)
			i += 4

			p.c++ // inc our gossipped count for this event

			if p.c < c.λlogn() { // piggyback this many times
				defer heap.Push(&c.pigBuffer, p) // we need to gossip this more, but our next loop should read another value.
			} else {
				log15.Debug("LAMBDA", "c", p.c, "len", len(c.pigBuffer))
			}
		}
	}

	c.conn.WriteTo(buf[:i], out.Who)
	c.bufPool.Put(buf)
}

func (c *Clique) ponder(e peer) {
	them, ok := c.mems[e.Who.String()]
	log15.Debug("considering", "e", e, "me", c.me)
	if ok && them.State == e.State {
		return // if we know this, don't think much
	}

	switch {
	case e.Who.String() == c.me.String() && e.State != Alive:
		c.time++
		e.State = Alive // we're here!
	case !ok:
		log15.Debug("adding", "who", e.Who)
		c.pals = append(c.pals, e.Who)
	case e.State == Confirm && e.State != Confirm:
		log15.Warn("killing", "who", e.Who)
		c.removePal(e.Who)
	}
	log15.Info("event yo", "e", e)
	c.mems[e.Who.String()] = &e // TODO ?? we don't know this until after upsert?
	c.upsertPiggy(e)
	go c.tell(e)
}

func (c *Clique) removePal(who *net.UDPAddr) {
	for i, p := range c.pals {
		if p.String() == who.String() {
			c.pals = append(c.pals[:i], c.pals[i+1:]...)
			break
		}
	}
}

func (c *Clique) upsertPiggy(e peer) {
	var n int
	for n = range c.pigBuffer {
		if c.pigBuffer[n].e.Who.String() == e.Who.String() {
			break // TODO O(n) kinda sucks
		}
	}

	if n == len(c.pigBuffer) { // not found
		log15.Debug("adding piggy", "e", e)
		heap.Push(&c.pigBuffer, pig{e: e})
		return
	} // else found one

	old := c.pigBuffer[n].e

	// { Alive M, inc = i } overrides
	//   - { Suspect M, inc = j }, i > j
	//   - { Alive M, inc = j }, i > j
	if (e.State == Alive &&
		((old.State == Suspect && e.Time > old.Time) ||
			(old.State == Alive && e.Time > old.Time))) ||

		// { Suspect M, inc = i } overrides
		//   - { Suspect M, inc = j }, i > j
		//   - { Alive M, inc = j }, i >= j
		(e.State == Suspect &&
			((old.State == Suspect && e.Time > old.Time) ||
				(old.State == Alive && e.Time >= old.Time))) ||

		// { Confirm M, inc = i } overrides
		//   - { Alive M, inc = j }, any j
		//   - { Suspect M, inc = j }, any j
		(e.State == Confirm && old.State != Confirm) {

		log15.Debug("overriding", "old", old.State, "new", e.State)
		c.pigBuffer[n].e.State = e.State
		c.pigBuffer[n].c = 0
		heap.Fix(&c.pigBuffer, n)
	}
}

func (c *Clique) tell(e peer) {
	for _, l := range c.listeners {
		select {
		case l <- Event{Who: e.Who, What: e.State}:
		default:
		}
	}
}
