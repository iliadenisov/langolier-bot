package tgclient

import (
	"context"
	"errors"
	"sync"

	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
)

// Relay bridges the interactive parts of the MTProto user-authorization flow to
// an operator channel (the service bot). Every method blocks until the operator
// provides the value or ctx is cancelled.
type Relay interface {
	// AskPhone requests the account phone number.
	AskPhone(ctx context.Context) (string, error)
	// AskCode requests the login code delivered by Telegram.
	AskCode(ctx context.Context) (string, error)
	// AskPassword requests the 2FA password. hint is the server-provided hint,
	// possibly empty.
	AskPassword(ctx context.Context, hint string) (string, error)
}

// errSignUpUnsupported is returned whenever Telegram asks the flow to register a
// new account; this bot only signs in existing accounts.
var errSignUpUnsupported = errors.New("tgclient: account sign-up is not supported")

// relayAuth adapts a Relay to auth.UserAuthenticator. It caches the phone number
// so an AUTH_RESTART retry of the flow does not re-prompt for it.
type relayAuth struct {
	relay Relay
	api   *tg.Client

	mu    sync.Mutex
	phone string
}

var _ auth.UserAuthenticator = (*relayAuth)(nil)

func (a *relayAuth) Phone(ctx context.Context) (string, error) {
	a.mu.Lock()
	cached := a.phone
	a.mu.Unlock()
	if cached != "" {
		return cached, nil
	}
	p, err := a.relay.AskPhone(ctx)
	if err != nil {
		return "", err
	}
	a.mu.Lock()
	a.phone = p
	a.mu.Unlock()
	return p, nil
}

func (a *relayAuth) Code(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
	return a.relay.AskCode(ctx)
}

func (a *relayAuth) Password(ctx context.Context) (string, error) {
	return a.relay.AskPassword(ctx, a.hint(ctx))
}

// hint returns the account's 2FA password hint, or "" if it cannot be fetched.
func (a *relayAuth) hint(ctx context.Context) string {
	if a.api == nil {
		return ""
	}
	p, err := a.api.AccountGetPassword(ctx)
	if err != nil {
		return ""
	}
	h, _ := p.GetHint()
	return h
}

func (a *relayAuth) AcceptTermsOfService(context.Context, tg.HelpTermsOfService) error {
	return errSignUpUnsupported
}

func (a *relayAuth) SignUp(context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errSignUpUnsupported
}
