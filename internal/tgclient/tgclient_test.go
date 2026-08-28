package tgclient

import (
	"reflect"
	"testing"
	"time"

	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
)

func TestMarkedIDScheme(t *testing.T) {
	// Channel marks live below -channelIDShift; chat marks above it. The cleaner
	// relies on this boundary to tell the two apart in OnDeleted(0, ...).
	if got := MarkChannel(555); got != -channelIDShift-555 {
		t.Fatalf("MarkChannel(555) = %d", got)
	}
	if got := MarkChat(500); got != -500 {
		t.Fatalf("MarkChat(500) = %d", got)
	}
	if MarkChannel(1) >= -channelIDShift {
		t.Error("channel mark not below the shift boundary")
	}
	if MarkChat(1_000_000) <= -channelIDShift {
		t.Error("chat mark not above the shift boundary")
	}
}

func TestMessageLink(t *testing.T) {
	c := &Client{}

	pub := Group{IsChannel: true, Username: "somegroup", rawID: 555}
	if got := c.MessageLink(pub, 12); got != "https://t.me/somegroup/12" {
		t.Errorf("public: %q", got)
	}

	priv := Group{IsChannel: true, rawID: 777}
	if got := c.MessageLink(priv, 34); got != "https://t.me/c/777/34" {
		t.Errorf("private: %q", got)
	}

	basic := Group{IsChannel: false, rawID: 42}
	if got := c.MessageLink(basic, 9); got != "" {
		t.Errorf("basic group must have no link, got %q", got)
	}
}

func entities(chats map[int64]*tg.Chat, channels map[int64]*tg.Channel) peer.Entities {
	return peer.NewEntities(map[int64]*tg.User{}, chats, channels)
}

func TestGroupFromElem(t *testing.T) {
	// basic group, healthy
	{
		g, ok := groupFromElem(dialogs.Elem{
			Peer:     &tg.InputPeerChat{ChatID: 10},
			Entities: entities(map[int64]*tg.Chat{10: {ID: 10, Title: "Basic"}}, nil),
		})
		if !ok || g.IsChannel || g.MarkedID != MarkChat(10) || g.Title != "Basic" {
			t.Fatalf("basic ok: %+v ok=%v", g, ok)
		}
	}
	// basic group, skipped states
	for name, ch := range map[string]*tg.Chat{
		"left":        {ID: 1, Left: true},
		"deactivated": {ID: 1, Deactivated: true},
		"migrated":    {ID: 1, MigratedTo: &tg.InputChannel{ChannelID: 2}},
	} {
		if _, ok := groupFromElem(dialogs.Elem{
			Peer:     &tg.InputPeerChat{ChatID: 1},
			Entities: entities(map[int64]*tg.Chat{1: ch}, nil),
		}); ok {
			t.Errorf("basic %s must be skipped", name)
		}
	}
	// megagroup, healthy, with username
	{
		ch := &tg.Channel{ID: 20, Title: "Mega", Megagroup: true}
		ch.SetUsername("mega")
		g, ok := groupFromElem(dialogs.Elem{
			Peer:     &tg.InputPeerChannel{ChannelID: 20, AccessHash: 999},
			Entities: entities(nil, map[int64]*tg.Channel{20: ch}),
		})
		if !ok || !g.IsChannel || g.MarkedID != MarkChannel(20) || g.Username != "mega" || g.accessHash != 999 {
			t.Fatalf("megagroup ok: %+v ok=%v", g, ok)
		}
	}
	// channel variants that must be skipped
	for name, ch := range map[string]*tg.Channel{
		"broadcast": {ID: 3, Broadcast: true, Megagroup: false},
		"not-mega":  {ID: 3, Megagroup: false},
		"left":      {ID: 3, Megagroup: true, Left: true},
	} {
		if _, ok := groupFromElem(dialogs.Elem{
			Peer:     &tg.InputPeerChannel{ChannelID: 3},
			Entities: entities(nil, map[int64]*tg.Channel{3: ch}),
		}); ok {
			t.Errorf("channel %s must be skipped", name)
		}
	}
	// unknown entity / user peer
	if _, ok := groupFromElem(dialogs.Elem{Peer: &tg.InputPeerUser{UserID: 1}}); ok {
		t.Error("user peer must be skipped")
	}
	if _, ok := groupFromElem(dialogs.Elem{
		Peer:     &tg.InputPeerChannel{ChannelID: 99},
		Entities: entities(nil, map[int64]*tg.Channel{}),
	}); ok {
		t.Error("missing channel entity must be skipped")
	}
}

func TestHandleNewMessage(t *testing.T) {
	c := &Client{}
	type call struct {
		marked int64
		id     int
		date   time.Time
		text   string
	}
	var got *call
	c.OnOwnMessage(func(m int64, id int, d time.Time, txt string) {
		got = &call{m, id, d, txt}
	})

	c.handleNewMessage(&tg.Message{
		Out: true, ID: 10, Date: 1_700_000_000, Message: "hi",
		PeerID: &tg.PeerChannel{ChannelID: 555},
	})
	if got == nil || got.marked != MarkChannel(555) || got.id != 10 || got.text != "hi" ||
		!got.date.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("channel own message: %+v", got)
	}

	got = nil
	c.handleNewMessage(&tg.Message{Out: true, ID: 7, PeerID: &tg.PeerChat{ChatID: 8}})
	if got == nil || got.marked != MarkChat(8) || got.id != 7 {
		t.Fatalf("basic own message: %+v", got)
	}

	got = nil
	c.handleNewMessage(&tg.Message{Out: false, ID: 1, PeerID: &tg.PeerChannel{ChannelID: 1}})
	if got != nil {
		t.Error("incoming message must be ignored")
	}

	got = nil
	c.handleNewMessage(&tg.Message{Out: true, ID: 1, PeerID: &tg.PeerUser{UserID: 1}})
	if got != nil {
		t.Error("private-chat message must be ignored")
	}
}

func TestChunkInts(t *testing.T) {
	if chunkInts(nil, 3) != nil || chunkInts([]int{}, 3) != nil {
		t.Error("empty input must yield nil")
	}
	if chunkInts([]int{1, 2, 3}, 0) != nil {
		t.Error("non-positive n must yield nil")
	}
	if got := chunkInts([]int{1, 2, 3, 4, 5}, 2); !reflect.DeepEqual(got, [][]int{{1, 2}, {3, 4}, {5}}) {
		t.Errorf("split by 2: %v", got)
	}
	if got := chunkInts([]int{1, 2}, 5); !reflect.DeepEqual(got, [][]int{{1, 2}}) {
		t.Errorf("smaller than n: %v", got)
	}
	if got := chunkInts([]int{1, 2, 3}, 3); !reflect.DeepEqual(got, [][]int{{1, 2, 3}}) {
		t.Errorf("exact n: %v", got)
	}
}

func TestMessagesOf(t *testing.T) {
	m := []tg.MessageClass{&tg.Message{ID: 1}}
	if got := messagesOf(&tg.MessagesMessages{Messages: m}); len(got) != 1 {
		t.Error("MessagesMessages")
	}
	if got := messagesOf(&tg.MessagesMessagesSlice{Messages: m}); len(got) != 1 {
		t.Error("MessagesMessagesSlice")
	}
	if got := messagesOf(&tg.MessagesChannelMessages{Messages: m}); len(got) != 1 {
		t.Error("MessagesChannelMessages")
	}
	if got := messagesOf(&tg.MessagesMessagesNotModified{}); got != nil {
		t.Error("unknown type must yield nil")
	}
}

func TestOrDefault(t *testing.T) {
	if orDefault("", "d") != "d" || orDefault("x", "d") != "x" {
		t.Fail()
	}
}
