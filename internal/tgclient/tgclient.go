// Package tgclient wraps the gotd/td MTProto user client used to enumerate the
// account's group chats, scan the account's own messages and delete them. All
// RPC traffic is paced by a rate limiter and a FLOOD_WAIT-aware waiter to keep
// the account safe from anti-flood bans.
package tgclient

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gotd/contrib/bbolt"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"
	"github.com/gotd/log/logzap"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// channelIDShift converts a bare channel id to the bot-API "marked" form.
const channelIDShift = 1_000_000_000_000

// deleteBatch is the maximum number of message ids per delete RPC.
const deleteBatch = 100

// searchPageDelay paces pagination of the startup own-message scan.
const searchPageDelay = time.Second

// sessionBucket is the bbolt bucket holding the MTProto session.
var sessionBucket = []byte("session")

// MarkChannel returns the bot-API marked id for a bare supergroup/channel id.
func MarkChannel(id int64) int64 { return -channelIDShift - id }

// MarkChat returns the bot-API marked id for a bare basic-group id.
func MarkChat(id int64) int64 { return -id }

// Group is a group chat the account belongs to that is eligible for cleanup:
// a basic group or a supergroup (megagroup). Broadcast channels and private
// chats are never represented here.
type Group struct {
	MarkedID  int64
	Title     string
	Username  string
	IsChannel bool // true: supergroup (channel), false: basic group

	rawID      int64
	accessHash int64
}

func (g Group) inputPeer() tg.InputPeerClass {
	if g.IsChannel {
		return &tg.InputPeerChannel{ChannelID: g.rawID, AccessHash: g.accessHash}
	}
	return &tg.InputPeerChat{ChatID: g.rawID}
}

func (g Group) inputChannel() tg.InputChannelClass {
	return &tg.InputChannel{ChannelID: g.rawID, AccessHash: g.accessHash}
}

// OwnMessageFunc is called for every outgoing message the account sends in a
// tracked group; date is the message send time.
type OwnMessageFunc func(markedID int64, msgID int, date time.Time, text string)

// DeletedMessagesFunc is called when messages are deleted. markedID is zero for
// the common (basic-group/private) message box, where ids are account-global.
type DeletedMessagesFunc func(markedID int64, ids []int)

// Options configures New.
type Options struct {
	DB      *bolt.DB
	AppID   int
	AppHash string
	Logger  *zap.Logger
	Relay   Relay

	// DeviceModel, AppVersion and SystemVersion are reported to Telegram on
	// connect and shown in the account's "Active Sessions" list. Empty fields
	// fall back to built-in defaults.
	DeviceModel   string
	AppVersion    string
	SystemVersion string
}

// Client is the running MTProto user client wrapper.
type Client struct {
	log        *zap.Logger
	relay      Relay
	tg         *telegram.Client
	api        *tg.Client
	gaps       *updates.Manager
	dispatcher tg.UpdateDispatcher
	waiter     *floodwait.Waiter
	limiter    *ratelimit.RateLimiter

	onOwn OwnMessageFunc
	onDel DeletedMessagesFunc

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan error
	self    *tg.User
	groups  map[int64]Group
}

// New builds the client. It does not connect; call Start.
func New(opts Options) (*Client, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	c := &Client{
		log:    opts.Logger,
		relay:  opts.Relay,
		groups: make(map[int64]Group),
	}

	hashes, err := newHashStore(opts.DB)
	if err != nil {
		return nil, fmt.Errorf("hash store: %w", err)
	}
	// gotd/contrib's bbolt session storage errors out ("bucket does not exist")
	// instead of reporting session.ErrNotFound when the bucket is missing, which
	// breaks the very first start. Pre-create it so an absent session key
	// resolves to "not found" and the auth flow runs.
	if err := opts.DB.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(sessionBucket)
		return e
	}); err != nil {
		return nil, fmt.Errorf("session bucket: %w", err)
	}
	session := bbolt.NewSessionStorage(opts.DB, "session", sessionBucket)
	state := bbolt.NewStateStorage(opts.DB)

	lg := logzap.New(opts.Logger.Named("gotd"))

	c.dispatcher = tg.NewUpdateDispatcher()
	c.registerHandlers()

	c.gaps = updates.New(updates.Config{
		Handler:          c.dispatcher,
		Storage:          state,
		AccessHasher:     hashes,
		UserAccessHasher: hashes,
		Logger:           lg,
	})

	c.waiter = floodwait.NewWaiter().WithMaxRetries(5).WithMaxWait(2 * time.Minute)
	c.limiter = ratelimit.New(rate.Every(time.Second), 3)

	c.tg = telegram.NewClient(opts.AppID, opts.AppHash, telegram.Options{
		Logger:         lg,
		SessionStorage: session,
		UpdateHandler:  c.gaps,
		Middlewares:    []telegram.Middleware{c.limiter, c.waiter},
		Device: telegram.DeviceConfig{
			DeviceModel:    orDefault(opts.DeviceModel, "Langolier"),
			AppVersion:     orDefault(opts.AppVersion, "dev"),
			SystemVersion:  orDefault(opts.SystemVersion, "Linux"),
			SystemLangCode: "en",
			LangCode:       "en",
		},
	})
	c.api = c.tg.API()
	return c, nil
}

// OnOwnMessage sets the outgoing-message callback. Call before Start.
func (c *Client) OnOwnMessage(fn OwnMessageFunc) {
	c.mu.Lock()
	c.onOwn = fn
	c.mu.Unlock()
}

// OnDeletedMessages sets the deletion callback. Call before Start.
func (c *Client) OnDeletedMessages(fn DeletedMessagesFunc) {
	c.mu.Lock()
	c.onDel = fn
	c.mu.Unlock()
}

func (c *Client) registerHandlers() {
	c.dispatcher.OnNewMessage(func(_ context.Context, _ tg.Entities, u *tg.UpdateNewMessage) error {
		c.handleNewMessage(u.Message)
		return nil
	})
	c.dispatcher.OnNewChannelMessage(func(_ context.Context, _ tg.Entities, u *tg.UpdateNewChannelMessage) error {
		c.handleNewMessage(u.Message)
		return nil
	})
	c.dispatcher.OnDeleteMessages(func(_ context.Context, _ tg.Entities, u *tg.UpdateDeleteMessages) error {
		c.fireDeleted(0, u.Messages)
		return nil
	})
	c.dispatcher.OnDeleteChannelMessages(func(_ context.Context, _ tg.Entities, u *tg.UpdateDeleteChannelMessages) error {
		c.fireDeleted(MarkChannel(u.ChannelID), u.Messages)
		return nil
	})
}

func (c *Client) handleNewMessage(mc tg.MessageClass) {
	m, ok := mc.(*tg.Message)
	if !ok || !m.Out {
		return
	}
	var marked int64
	switch p := m.PeerID.(type) {
	case *tg.PeerChat:
		marked = MarkChat(p.ChatID)
	case *tg.PeerChannel:
		marked = MarkChannel(p.ChannelID)
	default:
		return
	}
	c.mu.Lock()
	fn := c.onOwn
	c.mu.Unlock()
	if fn != nil {
		fn(marked, m.ID, time.Unix(int64(m.Date), 0), m.Message)
	}
}

func (c *Client) fireDeleted(marked int64, ids []int) {
	c.mu.Lock()
	fn := c.onDel
	c.mu.Unlock()
	if fn != nil && len(ids) > 0 {
		fn(marked, ids)
	}
}

// Start connects, runs the auth flow via the relay when needed, launches the
// updates manager and blocks in the background. It returns once the client is
// ready (or with the startup error). onReady, if set, runs in its own goroutine
// after readiness.
func (c *Client) Start(parent context.Context, onReady func(context.Context)) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("tgclient: already running")
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.running = true
	c.done = make(chan error, 1)
	done := c.done
	c.mu.Unlock()

	ready := make(chan error, 1)
	go func() {
		done <- c.waiter.Run(ctx, func(ctx context.Context) error {
			return c.tg.Run(ctx, func(ctx context.Context) error {
				if err := c.tg.Auth().IfNecessary(ctx, auth.NewFlow(relayAuth{c.relay}, auth.SendCodeOptions{})); err != nil {
					ready <- err
					return err
				}
				self, err := c.tg.Self(ctx)
				if err != nil {
					ready <- err
					return err
				}
				c.setSelf(self)

				grp, gctx := errgroup.WithContext(ctx)
				grp.Go(func() error {
					return c.gaps.Run(gctx, c.api, self.ID, updates.AuthOptions{})
				})
				ready <- nil
				if onReady != nil {
					go onReady(ctx)
				}
				<-ctx.Done()
				_ = grp.Wait()
				return ctx.Err()
			})
		})
	}()

	select {
	case err := <-ready:
		if err != nil {
			c.finish()
		}
		return err
	case err := <-done:
		c.finish()
		if err != nil {
			return err
		}
		return errors.New("tgclient: stopped during startup")
	}
}

// Stop cancels the client and waits for it to shut down.
func (c *Client) Stop() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	cancel, done := c.cancel, c.done
	c.mu.Unlock()

	cancel()
	err := <-done
	c.finish()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c *Client) finish() {
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()
}

func (c *Client) setSelf(u *tg.User) {
	c.mu.Lock()
	c.self = u
	c.mu.Unlock()
}

// Running reports whether the client is currently connected.
func (c *Client) Running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// ResolveGroups enumerates the account's dialogs and returns the eligible group
// chats, refreshing the internal cache used by Group.
func (c *Client) ResolveGroups(ctx context.Context) ([]Group, error) {
	var groups []Group
	err := dialogs.NewQueryBuilder(c.api).GetDialogs().BatchSize(100).ForEach(ctx, func(_ context.Context, e dialogs.Elem) error {
		g, ok := groupFromElem(e)
		if !ok {
			return nil
		}
		groups = append(groups, g)
		return nil
	})
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	for _, g := range groups {
		c.groups[g.MarkedID] = g
	}
	c.mu.Unlock()

	sort.Slice(groups, func(i, j int) bool { return groups[i].Title < groups[j].Title })
	return groups, nil
}

// Group returns a cached group by marked id.
func (c *Client) Group(markedID int64) (Group, bool) {
	c.mu.Lock()
	g, ok := c.groups[markedID]
	c.mu.Unlock()
	return g, ok
}

func groupFromElem(e dialogs.Elem) (Group, bool) {
	switch p := e.Peer.(type) {
	case *tg.InputPeerChat:
		ch, ok := e.Entities.Chat(p.ChatID)
		if !ok || ch.Left || ch.Deactivated || ch.MigratedTo != nil {
			return Group{}, false
		}
		return Group{
			MarkedID: MarkChat(ch.ID),
			Title:    ch.Title,
			rawID:    ch.ID,
		}, true
	case *tg.InputPeerChannel:
		ch, ok := e.Entities.Channel(p.ChannelID)
		if !ok || ch.Broadcast || !ch.Megagroup || ch.Left {
			return Group{}, false
		}
		username, _ := ch.GetUsername()
		return Group{
			MarkedID:   MarkChannel(ch.ID),
			Title:      ch.Title,
			Username:   username,
			IsChannel:  true,
			rawID:      ch.ID,
			accessHash: p.AccessHash,
		}, true
	default:
		return Group{}, false
	}
}

// ScanOwn pages through the account's own messages in g, oldest-first pacing,
// invoking cb for every message found.
func (c *Client) ScanOwn(ctx context.Context, g Group, cb func(msgID int, date time.Time)) error {
	offsetID := 0
	for {
		res, err := c.api.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:     g.inputPeer(),
			FromID:   &tg.InputPeerSelf{},
			Filter:   &tg.InputMessagesFilterEmpty{},
			Limit:    100,
			OffsetID: offsetID,
		})
		if err != nil {
			return err
		}
		msgs := messagesOf(res)
		if len(msgs) == 0 {
			return nil
		}
		last := 0
		for _, mc := range msgs {
			m, ok := mc.(*tg.Message)
			if !ok {
				continue
			}
			cb(m.ID, time.Unix(int64(m.Date), 0))
			last = m.ID
		}
		if last == 0 || last == offsetID {
			return nil
		}
		offsetID = last
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(searchPageDelay):
		}
	}
}

// Delete removes the given message ids from g in batches. It returns the ids
// whose batch RPC failed, alongside the last error.
func (c *Client) Delete(ctx context.Context, g Group, ids []int) (failed []int, err error) {
	for _, chunk := range chunkInts(ids, deleteBatch) {
		var e error
		if g.IsChannel {
			_, e = c.api.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
				Channel: g.inputChannel(),
				ID:      chunk,
			})
		} else {
			_, e = c.api.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{
				Revoke: true,
				ID:     chunk,
			})
		}
		if e != nil {
			failed = append(failed, chunk...)
			err = e
		}
	}
	return failed, err
}

// MessageLink builds a t.me deeplink to a message, or "" when the chat type has
// no per-message links (basic groups).
func (c *Client) MessageLink(g Group, msgID int) string {
	if !g.IsChannel {
		return ""
	}
	if g.Username != "" {
		return "https://t.me/" + g.Username + "/" + strconv.Itoa(msgID)
	}
	return "https://t.me/c/" + strconv.FormatInt(g.rawID, 10) + "/" + strconv.Itoa(msgID)
}

func messagesOf(res tg.MessagesMessagesClass) []tg.MessageClass {
	switch v := res.(type) {
	case *tg.MessagesMessages:
		return v.Messages
	case *tg.MessagesMessagesSlice:
		return v.Messages
	case *tg.MessagesChannelMessages:
		return v.Messages
	default:
		return nil
	}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func chunkInts(s []int, n int) [][]int {
	if n <= 0 || len(s) == 0 {
		return nil
	}
	var out [][]int
	for i := 0; i < len(s); i += n {
		end := i + n
		if end > len(s) {
			end = len(s)
		}
		out = append(out, s[i:end])
	}
	return out
}
