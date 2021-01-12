package backend

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
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
func (s *Services) StartGC(ctx context.Context, token string, options GCOptions) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "garbage_collection", &outcome, t0)

	leasePath, lease, err := s.Leases.GetLease(ctx, token)
	if err != nil {
		outcome = err.Error()
		return err
	}

	if err := CheckToken(token, lease.Token.Secret); err != nil {
		outcome = err.Error()
		return err
	}

	if leasePath != fmt.Sprintf("%s%s", options.Repository, "/") {
		err = fmt.Errorf("necessary lease on root of the repository `/` to run Garbage Collection")
		outcome = err.Error()
		return err
	}

	defer func() {
		if err = s.Leases.CancelLease(ctx, token); err != nil {
			// we are not really worried if something goes bad here but it still worth to log it
			gw.LogC(ctx, "gc", gw.LogInfo).Msg(fmt.Sprintf("error in cancelling the lease: %s", err.Error()))
		}
	}()

	baseArgs := []string{"gc", "-f", "-@"}
	if options.NumRevisions != 0 {
		baseArgs = append(baseArgs, "-r", strconv.Itoa(options.NumRevisions))
	}
	if !options.Timestamp.IsZero() {
		baseArgs = append(baseArgs, "-t", fmt.Sprintf("@%d", options.Timestamp.Unix()))
	}
	if options.DryRun {
		baseArgs = append(baseArgs, "-d")
	}
	if options.Verbose {
		baseArgs = append(baseArgs, "-l")
	}

	args := append(baseArgs, options.Repository)
	gctime := 24 * time.Hour
	// don't care about the cancel function
	gcctx, _ := context.WithTimeout(context.Background(), gctime)

	// we just start the GC and hold the lock on the token
	// we wait untill the GC does not finish
	// TODO set the results somewhere
	// we should keep the logs and other stuff
	go func() {
		s.Leases.WithLock(gcctx, options.Repository, func() error {
			var token_wg sync.WaitGroup
			token_wg.Add(1)
			defer token_wg.Done()
			go s.Leases.WithLock(ctx, token, func() error {
				// this is for the IsDoingGC check
				token_wg.Wait()
				return nil
			})
			fmt.Fprintf(os.Stdout, "Executing GC\n")
			cmd := exec.Command("cvmfs_server", args...)
			out, err := cmd.CombinedOutput()
			fmt.Fprintf(os.Stdout, "Executing GC: out: %s\n", out)
			if err != nil {
				return err
			}
			return nil
		})
	}()

	return nil
}

func (s *Services) IsDoingGC(ctx context.Context, token string) bool {
	return s.Leases.IsLocked(token)
}
