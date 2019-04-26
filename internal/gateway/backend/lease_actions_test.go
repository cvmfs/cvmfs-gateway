package backend

import (
	"context"
	"testing"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func TestLeaseActionsNewLease(t *testing.T) {
	backend := StartTestBackend("lease_actions_test", 1*time.Second)
	defer backend.Stop()

	t.Run("new lease busy", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer CancelLease(context.TODO(), backend, token1)
		token2, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err == nil {
			CancelLease(context.TODO(), backend, token2)
			t.Fatalf("new lease should not have been granted for busy path")
		}
	})
	t.Run("new lease expired", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Microsecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer CancelLease(context.TODO(), backend, token1)
		time.Sleep(backend.Config.MaxLeaseTime)
		if _, err := NewLease(context.TODO(), backend, keyID, leasePath); err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
	})
	t.Run("new lease conflict", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer CancelLease(context.TODO(), backend, token1)
		token2, err := NewLease(context.TODO(), backend, keyID, leasePath+"/below")
		if err == nil {
			CancelLease(context.TODO(), backend, token2)
			t.Fatalf("new lease should not have been granted for conflicting path")
		}
	})
}

func TestLeaseActionsCancelLease(t *testing.T) {
	backend := StartTestBackend("lease_actions_test", 1*time.Second)
	defer backend.Stop()

	t.Run("remove existing lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if err := CancelLease(context.TODO(), backend, token1); err != nil {
			t.Fatalf("could not cancel existing lease: %v", err)
		}
	})
	t.Run("remove nonexisting lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if err := CancelLease(context.TODO(), backend, token1); err != nil {
			t.Fatalf("could not cancel existing lease: %v", err)
		}
		if CancelLease(context.TODO(), backend, token1) == nil {
			t.Fatalf("cancel operation should have failed for nonexisting lease")
		}
	})
}

func TestLeaseActionsGetLease(t *testing.T) {
	backend := StartTestBackend("lease_actions_test", 1*time.Second)
	defer backend.Stop()

	t.Run("get valid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		lease, err := GetLease(context.TODO(), backend, token1)
		if err != nil {
			t.Fatalf("could not query existing lease: %v", err)
		}
		if lease.KeyID != keyID && lease.LeasePath != leasePath {
			t.Fatalf("lease query result is invalid: %v", lease)
		}
		defer CancelLease(context.TODO(), backend, token1)
	})
	t.Run("get expired lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Microsecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		time.Sleep(2 * backend.Config.MaxLeaseTime)
		_, err = GetLease(context.TODO(), backend, token1)
		if err == nil {
			t.Fatalf("query should not succeed for expired leases: %v", err)
		}
		if _, ok := err.(ExpiredTokenError); !ok {
			t.Fatalf("query should have returned an ExpiredTokenError. Instead: %v", err)
		}
	})
	t.Run("get invalid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		_, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		token2, err := NewLeaseToken(leasePath, time.Second)
		if err != nil {
			t.Fatalf("could not generate second token")
		}
		_, err = GetLease(context.TODO(), backend, token2.TokenStr)
		if err == nil {
			t.Fatalf("query should not succeed with invalid token: %v", err)
		}
		if _, ok := err.(InvalidLeaseError); !ok {
			t.Fatalf("query should have returned an InvalidLeaseError. Instead: %v", err)
		}
	})
}

func TestLeaseActionsCommitLease(t *testing.T) {
	backend := StartTestBackend("lease_actions_test", 1*time.Second)
	defer backend.Stop()

	t.Run("commit valid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if err := CommitLease(
			context.TODO(), backend, token, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Channel:     "mychannel",
				Description: "this is a tag",
			}); err != nil {
			t.Fatalf("could not commit existing lease: %v", err)
			CancelLease(context.TODO(), backend, token)
		}
	})
	t.Run("commit invalid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		leasePath := "test2.repo.org/some/path"
		token, err := NewLeaseToken(leasePath, backend.Config.MaxLeaseTime)
		if err != nil {
			t.Fatalf("could not obtain new lease token: %v", err)
		}
		if err := CommitLease(
			context.TODO(), backend, token.TokenStr, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Channel:     "mychannel",
				Description: "this is a tag",
			}); err == nil {
			t.Fatalf("invalid lease should not have been accepted for commit")
		}
	})
	t.Run("commit expired lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Millisecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token, err := NewLease(context.TODO(), backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		time.Sleep(2 * backend.Config.MaxLeaseTime)
		if CommitLease(
			context.TODO(), backend, token, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Channel:     "mychannel",
				Description: "this is a tag",
			}) == nil {
			t.Fatalf("expired lease should not have been accepted for commit")
		}
	})
}
