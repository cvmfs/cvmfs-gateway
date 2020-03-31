package backend

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// GCOptions represents the different options supplied for a garbace collection run
type GCOptions struct {
	Repository   string    `json:"repo"`
	NumRevisions int       `json:"num_revisions"`
	Timestamp    time.Time `json:"timestamp"`
	DryRun       bool      `json:"dry_run"`
	Verbose      bool      `json:"verbose"`
}

// RunGC triggers garbage collection on the specified repository
func (s *Services) RunGC(ctx context.Context, token string, options GCOptions) (string, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "garbage_collection", &outcome, t0)

	leasePath, lease, err := s.Leases.GetLease(ctx, token)
	if err != nil {
		outcome = err.Error()
		return "", err
	}

	if err := CheckToken(token, lease.Token.Secret); err != nil {
		outcome = err.Error()
		return "", err
	}

	if leasePath != "/" {
		err = fmt.Errorf("necessary lease on root of the repository `/` to run Garbage Collection")
		outcome = err.Error()
		return "", err
	}

	baseArgs := []string{"gc", "-f"}
	if options.NumRevisions != 0 {
		baseArgs = append(baseArgs, "-r", strconv.Itoa(options.NumRevisions))
	}
	if !options.Timestamp.IsZero() {
		baseArgs = append(baseArgs, "-t", options.Timestamp.String())
	}
	if options.DryRun {
		baseArgs = append(baseArgs, "-d")
	}
	if options.Verbose {
		baseArgs = append(baseArgs, "-l")
	}

	var output string
	args := append(baseArgs, options.Repository)
	if err := s.Leases.WithLock(ctx, options.Repository, func() error {
		cmd := exec.Command("cvmfs_server", args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return err
		}
		output = string(out)
		return nil
	}); err != nil {
		outcome = err.Error()
		return "", err
	}

	if err = s.Leases.CancelLease(ctx, token); err != nil {
		// we are not really worried if something goes bad here but it still worth to log it
		gw.LogC(ctx, "gc", gw.LogInfo).Msg(fmt.Sprintf("error in cancelling the lease: %s", err.Error()))
	}

	return output, nil
}
