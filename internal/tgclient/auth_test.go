package tgclient

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/tg"
)

type fakeRelay struct {
	phone      string
	phoneCalls int
}

func (f *fakeRelay) AskPhone(context.Context) (string, error) {
	f.phoneCalls++
	return f.phone, nil
}
func (f *fakeRelay) AskCode(context.Context) (string, error)             { return "", nil }
func (f *fakeRelay) AskPassword(context.Context, string) (string, error) { return "", nil }

func TestRelayAuthPhoneCached(t *testing.T) {
	f := &fakeRelay{phone: "+15551234567"}
	a := &relayAuth{relay: f}

	for i := 0; i < 3; i++ {
		p, err := a.Phone(context.Background())
		if err != nil || p != "+15551234567" {
			t.Fatalf("Phone() = %q, %v", p, err)
		}
	}
	if f.phoneCalls != 1 {
		t.Fatalf("AskPhone called %d times, want 1 (cached)", f.phoneCalls)
	}
}

func TestRelayAuthHintWithoutAPI(t *testing.T) {
	a := &relayAuth{relay: &fakeRelay{}}
	if h := a.hint(context.Background()); h != "" {
		t.Fatalf("hint without api = %q, want empty", h)
	}
}

func TestRelayAuthSignUpUnsupported(t *testing.T) {
	a := &relayAuth{relay: &fakeRelay{}}
	if err := a.AcceptTermsOfService(context.Background(), tg.HelpTermsOfService{}); !errors.Is(err, errSignUpUnsupported) {
		t.Errorf("AcceptTermsOfService err = %v", err)
	}
	if _, err := a.SignUp(context.Background()); !errors.Is(err, errSignUpUnsupported) {
		t.Errorf("SignUp err = %v", err)
	}
}
