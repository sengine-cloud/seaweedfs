package weed_server

// The filer serves an aggregated SubscribeMetadata from its filer-local stream
// while it knows of no peers. That is fine on a one-filer cluster and wrong the
// moment a peer joins, because the decision used to be made once per stream and
// a metadata subscription outlives the peer discovery that invalidates it.

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// startSoloAggregator gives the harness an aggregator that tracks only this
// filer, which is what a single-filer cluster looks like and what a filer looks
// like before the master has told it about anybody else.
func (h *subscribeHarness) startSoloAggregator() *filer.MetaAggregator {
	ma := filer.NewMetaAggregator(h.f, testSelfAddress, nil)
	ma.TrackPeerForTesting(testSelfAddress)
	h.f.MetaAggregator = ma
	h.t.Cleanup(ma.MetaLogBuffer.ShutdownLogBuffer)
	return ma
}

// TestSubscribeMetadataServesLocallyWithoutPeers pins the half that must keep
// working: with no peers the aggregated request is answered from the local
// stream, including this filer's own writes, rather than failing or stalling.
func TestSubscribeMetadataServesLocallyWithoutPeers(t *testing.T) {
	h := newSubscribeHarness(t)
	h.startSoloAggregator()

	r := h.subscribeAggregated(h.base)
	h.append(h.base + 1)
	h.append(h.base + 2)

	waitForEvents(t, r, []int64{h.base + 1, h.base + 2}, 5*time.Second)
}

// TestSubscribeMetadataEndsWhenPeersAppear is the regression. A subscription
// that started before the filer knew of any peer must not keep serving a
// single filer's view once one joins: it ends, so the client reconnects onto
// the aggregated stream.
func TestSubscribeMetadataEndsWhenPeersAppear(t *testing.T) {
	h := newSubscribeHarness(t)
	ma := h.startSoloAggregator()

	r := h.subscribeAggregated(h.base)
	h.append(h.base + 1)
	waitForEvents(t, r, []int64{h.base + 1}, 5*time.Second)

	// Still running: no peer, nothing to upgrade to.
	select {
	case err := <-r.done:
		t.Fatalf("subscription ended before any peer appeared: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	ma.TrackPeerForTesting(testPeerAddress)

	select {
	case err := <-r.done:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("ended with %v, want a codes.Unavailable reconnect signal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription kept serving the local stream after a peer appeared")
	}
}

// TestWaitForRemotePeersNoLostWakeup covers the ordering the fix depends on: a
// peer that arrives between taking the change channel and reading the peer set
// must still be seen. Adding the peer before the wait even starts is the
// tightest version of that race.
func TestWaitForRemotePeersNoLostWakeup(t *testing.T) {
	ma := filer.NewMetaAggregator(nil, testSelfAddress, nil)
	t.Cleanup(ma.MetaLogBuffer.ShutdownLogBuffer)
	ma.TrackPeerForTesting(testSelfAddress)

	// The channel is taken while the set is still peerless, then the peer lands
	// before anyone waits on it.
	_ = ma.PeerSetChangedChan()
	ma.TrackPeerForTesting(testPeerAddress)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !waitForRemotePeers(ctx, ma) {
		t.Fatal("missed a peer that arrived before the wait")
	}
}

// TestWaitForRemotePeersIgnoresSelf guards the one wake that means nothing: a
// filer registers itself with its own aggregator at startup, and that must not
// read as the cluster having grown.
func TestWaitForRemotePeersIgnoresSelf(t *testing.T) {
	ma := filer.NewMetaAggregator(nil, testSelfAddress, nil)
	t.Cleanup(ma.MetaLogBuffer.ShutdownLogBuffer)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() {
		time.Sleep(20 * time.Millisecond)
		ma.TrackPeerForTesting(testSelfAddress)
		ma.TrackPeerForTesting(pb.ServerAddress(testSelfAddress))
	}()

	if waitForRemotePeers(ctx, ma) {
		t.Fatal("treated this filer's own registration as a remote peer")
	}
}
