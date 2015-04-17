package swimmy

import (
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/iron-io/go/vendored/gopkg.in/inconshreveable/log15.v2"
)

func init() {
	log15.Root().SetHandler(log15.LvlFilterHandler(log15.LvlCrit, log15.StderrHandler))
}

// TODO custom base conf
func newN(t *testing.T, n int, conf *Config) []*Clique {
	var cliques []*Clique
	for i := 0; i < n; i++ {
		c, err := New(conf)
		if err != nil {
			t.Fatal(err)
		}
		conf.Seeds = append(conf.Seeds, c.Me().(*net.UDPAddr))
		conf.Port++

		cliques = append(cliques, c)
		log15.Info("cf", "conf", conf)
	}
	return cliques
}

func cleanup(cs []*Clique) {
	for _, c := range cs {
		c.Die()
	}
}

func TestBasic(t *testing.T) {
	conf := &Config{
		Host:           "127.0.0.1",
		Port:           8500,
		GossipInterval: 1000 * time.Millisecond,
	}
	cs := newN(t, 2, conf)
	defer cleanup(cs)

	// Seed() is pretty fast
	<-time.Tick(2 * conf.GossipInterval)

	for _, c := range cs {
		if len(c.Alive(true)) < 2 {
			t.Error("expected 2 alive, got:", c.Alive(true), "me:", c.Me())
		}
	}
}

func TestStableReads(t *testing.T) {
	runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(1)
	conf := &Config{
		Host:           "127.0.0.1",
		Port:           8500,
		GossipInterval: 200 * time.Millisecond,
	}
	n := 50
	cs := newN(t, n, conf)
	defer cleanup(cs)

	<-time.Tick(1 * conf.GossipInterval)

	for _, c := range cs {
		a := len(c.Alive(true))
		if a != n {
			t.Error("expected", n, "alive, got:", a)
		}
	}
}

func TestSmallTimeout(t *testing.T) {
	conf := &Config{
		Host:           "127.0.0.1",
		Port:           8500,
		GossipInterval: 200 * time.Millisecond,
	}
	n := 3
	cs := newN(t, n, conf)
	defer cleanup(cs[1:])

	<-time.Tick(1 * conf.GossipInterval)

	cs[0].Die()

	<-time.Tick(4 * conf.GossipInterval)

	for _, c := range cs[1:] {
		a := len(c.Alive(true))
		if a != n-1 {
			t.Error("expected", n-1, "alive, got:", a, "me", c.Me())
		}
	}
}

func TestFail(t *testing.T) {
	runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(1)
	conf := &Config{
		Host:           "127.0.0.1",
		Port:           8500,
		GossipInterval: 200 * time.Millisecond,
	}
	n := 100
	cs := newN(t, n, conf)
	defer cleanup(cs[1:])

	<-time.Tick(25 * conf.GossipInterval)

	for _, c := range cs {
		if len(c.Alive(true)) != n {
			t.Error("expected", n, "alive, got:", len(c.Alive(true)))
		}
	}

	// nuke one
	cs[0].Die()

	<-time.Tick(25 * conf.GossipInterval) // time bounded completeness

	for _, c := range cs[1:] {
		if len(c.Alive(true)) != n-1 {
			t.Error("expected", n-1, "alive, got:", len(c.Alive(true)))
		}
	}

	c2s := newN(t, 1, conf)
	defer cleanup(c2s)

	<-time.Tick(25 * conf.GossipInterval)

	for _, c := range append(cs[1:], c2s...) {
		if len(c.Alive(true)) != n {
			t.Error("expected", n, "alive, got:", len(c.Alive(true)))
		}
	}
}
