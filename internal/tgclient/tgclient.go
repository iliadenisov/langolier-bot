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
	"github.com/gotd/td/tgerr"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// MembershipFunc is called when something membership-related happens: the
// account left, was removed from, or (re)joined a chat. It carries no detail;
// the callee is expected to reconcile against the current dialog list.
type MembershipFunc func()

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

// errNotRunning is returned by API calls made while the client is stopped.
var errNotRunning = errors.New("tgclient: client is not running")

// Client wraps the MTProto user client. The gotd stack (telegram.Client,
// updates manager, waiter) is single-use, so it is rebuilt on every Start.
type Client struct {
	log   *zap.Logger
	relay Relay
	opts  Options

	onOwn        OwnMessageFunc
	onDel        DeletedMessagesFunc
	onMembership MembershipFunc

	mu     sync.Mutex
	groups map[int64]Group
	self   *tg.User

	// per-run state, set by buildStack and cleared by reset.
	running    bool
	tg         *telegram.Client
	api        *tg.Client
	gaps       *updates.Manager
	dispatcher tg.UpdateDispatcher
	waiter     *floodwait.Waiter
	cancel     context.CancelFunc
	done       chan struct{} // closed by the run goroutine on exit
	runErr     error         // set before done is closed
}

// New validates options and prepares the bbolt state. It does not connect; call
// Start.
func New(opts Options) (*Client, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
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
	return &Client{
		log:    opts.Logger,
		relay:  opts.Relay,
		opts:   opts,
		groups: make(map[int64]Group),
	}, nil
}

// buildStack constructs a fresh gotd stack. Called under c.mu at the start of
// every Start because telegram.Client and updates.Manager cannot be reused once
// their Run has returned.
func (c *Client) buildStack() error {
	hashes, err := newHashStore(c.opts.DB)
	if err != nil {
		return fmt.Errorf("hash store: %w", err)
	}
	session := bbolt.NewSessionStorage(c.opts.DB, "session", sessionBucket)
	state := bbolt.NewStateStorage(c.opts.DB)
	// gotd logs connection housekeeping ("Salt updated", "SessionInit",
	// "Key already exists", …) at Info, which drowns the operator log. Cap the
	// stack logger at Warn so only warnings and errors from gotd survive.
	lg := logzap.New(c.log.Named("gotd").WithOptions(zap.IncreaseLevel(zapcore.WarnLevel)))

	c.dispatcher = tg.NewUpdateDispatcher()
	c.registerHandlers()

	c.gaps = updates.New(updates.Config{
		Handler:          c.dispatcher,
		Storage:          state,
		AccessHasher:     hashes,
		UserAccessHasher: hashes,
		Logger:           lg,
		OnChannelInaccessible: func(channelID int64) {
			c.log.Info("channel inaccessible, membership reconcile due",
				zap.Int64("channel_id", channelID))
			c.fireMembership()
		},
	})
	c.waiter = floodwait.NewWaiter().WithMaxRetries(5).WithMaxWait(2 * time.Minute)

	c.tg = telegram.NewClient(c.opts.AppID, c.opts.AppHash, telegram.Options{
		Logger:         lg,
		SessionStorage: session,
		UpdateHandler:  c.gaps,
		Middlewares: []telegram.Middleware{
			ratelimit.New(rate.Every(time.Second), 3),
			c.waiter,
		},
		Device: telegram.DeviceConfig{
			DeviceModel:    orDefault(c.opts.DeviceModel, "Langolier"),
			AppVersion:     orDefault(c.opts.AppVersion, "dev"),
			SystemVersion:  orDefault(c.opts.SystemVersion, "Linux"),
			SystemLangCode: "en",
			LangCode:       "en",
		},
	})
	c.api = c.tg.API()
	return nil
}

// reset clears per-run state so the next Start builds a clean stack. runErr is
// kept: Start / Stop read it via takeRunErr right after reset.
func (c *Client) reset() {
	c.mu.Lock()
	c.running = false
	c.tg = nil
	c.api = nil
	c.gaps = nil
	c.waiter = nil
	c.cancel = nil
	c.done = nil
	c.mu.Unlock()
}

// apiRef returns the live API client, or an error when stopped.
func (c *Client) apiRef() (*tg.Client, error) {
	c.mu.Lock()
	api := c.api
	c.mu.Unlock()
	if api == nil {
		return nil, errNotRunning
	}
	return api, nil
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

// OnMembership sets the membership-change callback. Call before Start.
func (c *Client) OnMembership(fn MembershipFunc) {
	c.mu.Lock()
	c.onMembership = fn
	c.mu.Unlock()
}

func (c *Client) fireMembership() {
	c.mu.Lock()
	fn := c.onMembership
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
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
	if svc, ok := mc.(*tg.MessageService); ok {
		if isMembershipAction(svc.Action) {
			c.fireMembership()
		}
		return
	}
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

// isMembershipAction reports whether a service-message action means someone
// joined or left the chat — a cue that the account's own membership may have
// changed and a reconcile is due.
func isMembershipAction(a tg.MessageActionClass) bool {
	switch a.(type) {
	case *tg.MessageActionChatDeleteUser,
		*tg.MessageActionChatAddUser,
		*tg.MessageActionChatJoinedByLink,
		*tg.MessageActionChatJoinedByRequest:
		return true
	default:
		return false
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

// authRestartLimit bounds how many times a fresh auth flow is retried after
// Telegram answers AUTH_RESTART (a stale unfinished login on the account).
const authRestartLimit = 3

// Start builds a fresh gotd stack, runs the auth flow via the relay when needed,
// launches the updates manager and blocks it in the background. It returns once
// the client is ready or with the startup error; on any failure the client is
// fully reset so a later Start starts clean. onReady, if set, runs in its own
// goroutine after readiness.
func (c *Client) Start(parent context.Context, onReady func(context.Context)) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("tgclient: already running")
	}
	if err := c.buildStack(); err != nil {
		c.mu.Unlock()
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.running = true
	c.runErr = nil
	c.done = make(chan struct{})
	done := c.done
	tgClient, waiter := c.tg, c.waiter
	c.mu.Unlock()

	ready := make(chan error, 1)
	go func() {
		err := waiter.Run(ctx, func(ctx context.Context) error {
			return tgClient.Run(ctx, func(ctx context.Context) error {
				if err := c.authorize(ctx); err != nil {
					ready <- err
					return err
				}
				self, err := tgClient.Self(ctx)
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
		c.mu.Lock()
		c.runErr = err
		c.mu.Unlock()
		close(done)
	}()

	select {
	case err := <-ready:
		if err != nil {
			cancel()
			<-done
			c.reset()
			return err
		}
		return nil
	case <-done:
		cancel()
		err := c.takeRunErr()
		c.reset()
		if err != nil {
			return err
		}
		return errors.New("tgclient: stopped during startup")
	}
}

func (c *Client) takeRunErr() error {
	c.mu.Lock()
	err := c.runErr
	c.mu.Unlock()
	return err
}

// authorize runs the user-authorization flow. It retries the whole flow on
// AUTH_RESTART (the phone is cached, so only the code is re-requested) and hands
// off to retryPassword when the 2FA password is rejected.
func (c *Client) authorize(ctx context.Context) error {
	authr := &relayAuth{relay: c.relay, api: c.api}

	err := c.tg.Auth().IfNecessary(ctx, auth.NewFlow(authr, auth.SendCodeOptions{}))
	for attempt := 0; attempt < authRestartLimit && tgerr.Is(err, "AUTH_RESTART"); attempt++ {
		c.log.Warn("auth restarted by Telegram, retrying the flow")
		err = c.tg.Auth().IfNecessary(ctx, auth.NewFlow(authr, auth.SendCodeOptions{}))
	}
	if errors.Is(err, auth.ErrPasswordInvalid) {
		return c.retryPassword(ctx, authr)
	}
	return err
}

// retryPassword keeps asking the operator for the 2FA password until one is
// accepted, ctx is cancelled, or a non-password error occurs. It is entered only
// after the flow reported an invalid password, at which point the phone and code
// are already accepted and only account.checkPassword has to succeed.
func (c *Client) retryPassword(ctx context.Context, authr *relayAuth) error {
	hint := authr.hint(ctx)
	for {
		pw, err := c.relay.AskPassword(ctx, hint)
		if err != nil {
			return err
		}
		_, err = c.tg.Auth().Password(ctx, pw)
		if err == nil {
			return nil
		}
		if !errors.Is(err, auth.ErrPasswordInvalid) {
			return err
		}
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
	<-done
	err := c.takeRunErr()
	c.reset()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
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
	api, err := c.apiRef()
	if err != nil {
		return nil, err
	}
	var groups []Group
	err = dialogs.NewQueryBuilder(api).GetDialogs().BatchSize(100).ForEach(ctx, func(_ context.Context, e dialogs.Elem) error {
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

// searchPageLimit is the page size of the own-message history scan.
const searchPageLimit = 100

// ScanOwn walks the account's own messages in g newest-first, paging backwards
// with messages.search and invoking cb for every message found. It stops when
// the server returns an empty page or stops advancing the offset. Per-page
// progress is logged at Debug and the outcome (page count, messages indexed,
// server-reported total) at Info, so an unexpectedly shallow scan can be told
// apart from a genuine end-of-history by reading the log.
func (c *Client) ScanOwn(ctx context.Context, g Group, cb func(msgID int, date time.Time)) error {
	api, err := c.apiRef()
	if err != nil {
		return err
	}
	var offsetID, pages, indexed int
	for {
		res, err := api.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:     g.inputPeer(),
			FromID:   &tg.InputPeerSelf{},
			Filter:   &tg.InputMessagesFilterEmpty{},
			Limit:    searchPageLimit,
			OffsetID: offsetID,
		})
		if err != nil {
			return err
		}
		msgs := messagesOf(res)
		serverCount := searchTotal(res)
		pages++

		var counted, minID int
		for _, mc := range msgs {
			m, ok := mc.(*tg.Message)
			if !ok {
				continue // service message: still counts towards the offset below
			}
			cb(m.ID, time.Unix(int64(m.Date), 0))
			counted++
			if minID == 0 || m.ID < minID {
				minID = m.ID
			}
		}
		indexed += counted

		c.log.Debug("own-message scan page",
			zap.String("title", g.Title),
			zap.Int("page", pages),
			zap.Int("offset_id", offsetID),
			zap.Int("returned", len(msgs)),
			zap.Int("counted", counted),
			zap.Int("page_min_id", minID),
			zap.Int("server_count", serverCount),
			zap.Int("indexed", indexed),
		)

		// End of history: the server returned nothing, or a page with no real
		// messages, or it will not move the offset any further back.
		if len(msgs) == 0 || minID == 0 || minID == offsetID {
			lvl := c.log.Info
			reason := "end of history"
			if serverCount > 0 && indexed < serverCount {
				lvl = c.log.Warn
				reason = "stopped before server-reported total"
			}
			lvl("own-message scan finished",
				zap.String("title", g.Title),
				zap.String("reason", reason),
				zap.Int("pages", pages),
				zap.Int("indexed", indexed),
				zap.Int("server_count", serverCount),
				zap.Int("last_offset_id", offsetID),
				zap.Int("last_page_returned", len(msgs)),
			)
			return nil
		}
		offsetID = minID
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
	api, err := c.apiRef()
	if err != nil {
		return nil, err
	}
	for _, chunk := range chunkInts(ids, deleteBatch) {
		var e error
		if g.IsChannel {
			_, e = api.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
				Channel: g.inputChannel(),
				ID:      chunk,
			})
		} else {
			_, e = api.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{
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

// searchTotal reports the server's total match count for a messages.search
// response. MessagesMessages carries the whole result set in one page and has
// no separate counter, so its length is the total. Unknown types report -1.
func searchTotal(res tg.MessagesMessagesClass) int {
	switch v := res.(type) {
	case *tg.MessagesMessages:
		return len(v.Messages)
	case *tg.MessagesMessagesSlice:
		return v.Count
	case *tg.MessagesChannelMessages:
		return v.Count
	default:
		return -1
	}
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
