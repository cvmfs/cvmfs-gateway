package receiver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// Error is returned by the various receiver commands in case of error
type Error string

func (e Error) Error() string {
	return string(e)
}

// receiverOp is used to identify the different operation performed
// by the cvmfs_receiver process
type receiverOp int32

// The different operations are defined as constants. The numbering
// must match (enum receiver::Request from "cvmfs.git/cvmfs/receiver/reactor.h")
const (
	receiverQuit receiverOp = iota
	receiverEcho
	receiverGenerateToken // Unused
	receiverGetTokenID    // Unused
	receiverCheckToken    // Unused
	receiverSubmitPayload
	receiverCommit
	receiverError // Unused
)

// Receiver contains the operations that "receiver" worker processes perform
type Receiver interface {
	Quit() error
	Echo() error
	SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error
	Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error
}

// NewReceiver is the factory method for Receiver types
func NewReceiver(ctx context.Context, execPath string, mock bool) (Receiver, error) {
	if mock {
		return NewMockReceiver(ctx)
	}

	return NewCvmfsReceiver(ctx, execPath)
}

// CvmfsReceiver spawns an external cvmfs_receiver worker process
type CvmfsReceiver struct {
	worker *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	ctx    context.Context
}

// NewCvmfsReceiver will spawn an external cvmfs_receiver worker process and wait for a command
func NewCvmfsReceiver(ctx context.Context, execPath string) (*CvmfsReceiver, error) {
	if _, err := os.Stat(execPath); os.IsNotExist(err) {
		return nil, errors.Wrap(err, "worker process executable not found")
	}

	cmd := exec.Command(execPath, "-i", strconv.Itoa(3), "-o", strconv.Itoa(4))

	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not create stdin pipe")
	}
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not create stdout pipe")
	}

	cmd.ExtraFiles = []*os.File{stdinRead, stdoutWrite}

	if err := cmd.Start(); err != nil {
		return nil, errors.Wrap(err, "could not start worker process")
	}

	gw.LogC(ctx, "receiver", gw.LogDebug).
		Str("command", "start").
		Msg("worker process ready")

	return &CvmfsReceiver{worker: cmd, stdin: stdinWrite, stdout: stdoutRead, ctx: ctx}, nil
}

// Quit command is sent to the worker
func (r *CvmfsReceiver) Quit() error {
	defer func() {
		r.stdin.Close()
		r.stdout.Close()
	}()

	if _, err := r.call(receiverQuit, []byte{}, nil); err != nil {
		return errors.Wrap(err, "worker 'quit' call failed")
	}

	if err := r.worker.Wait(); err != nil {
		return errors.Wrap(err, "waiting for worker process failed")
	}

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "quit").
		Msg("worker process has stopped")

	return nil
}

// Echo command is sent to the worker
func (r *CvmfsReceiver) Echo() error {
	rep, err := r.call(receiverEcho, []byte("Ping"), nil)
	if err != nil {
		return errors.Wrap(err, "worker 'echo' call failed")
	}
	reply := string(rep)

	if !strings.HasPrefix(reply, "PID: ") {
		return fmt.Errorf("invalid 'echo' reply received: %v", reply)
	}

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "echo").
		Msgf("reply: %v", reply)

	return nil
}

// SubmitPayload command is sent to the worker
func (r *CvmfsReceiver) SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error {
	req := map[string]interface{}{"path": leasePath, "digest": digest, "header_size": headerSize}
	buf, err := json.Marshal(&req)
	if err != nil {
		return errors.Wrap(err, "request encoding failed")
	}
	reply, err := r.call(receiverSubmitPayload, buf, payload)
	if err != nil {
		return errors.Wrap(err, "worker 'payload submission' call failed")
	}

	result := toReceiverError(reply)

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "submit payload").
		Msgf("result: %v", result)

	return result
}

// Commit command is sent to the worker
func (r *CvmfsReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
	req := map[string]interface{}{
		"lease_path":      leasePath,
		"old_root_hash":   oldRootHash,
		"new_root_hash":   newRootHash,
		"tag_name":        tag.Name,
		"tag_channel":     tag.Channel,
		"tag_description": tag.Description,
	}
	buf, err := json.Marshal(&req)
	if err != nil {
		return errors.Wrap(err, "request encoding failed")
	}

	reply, err := r.call(receiverCommit, buf, nil)
	if err != nil {
		return errors.Wrap(err, "worker 'commit' call failed")
	}

	result := toReceiverError(reply)

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "commit").
		Msgf("result: %v", result)

	return result
}

func (r *CvmfsReceiver) call(reqID receiverOp, msg []byte, payload io.Reader) ([]byte, error) {
	if err := r.request(reqID, msg, payload); err != nil {
		return nil, err
	}
	return r.reply()
}

func (r *CvmfsReceiver) request(reqID receiverOp, msg []byte, payload io.Reader) error {
	if err := binary.Write(r.stdin, binary.LittleEndian, reqID); err != nil {
		return errors.Wrap(err, "could not write request id")
	}
	if err := binary.Write(r.stdin, binary.LittleEndian, int32(len(msg))); err != nil {
		return errors.Wrap(err, "could not write request size")
	}
	if _, err := r.stdin.Write(msg); err != nil {
		return errors.Wrap(err, "could not write request body")
	}
	if payload != nil {
		if _, err := io.Copy(r.stdin, payload); err != nil {
			return errors.Wrap(err, "could not write request payload")
		}
	}
	return nil
}

func (r *CvmfsReceiver) reply() ([]byte, error) {
	var repSize int32
	if err := binary.Read(r.stdout, binary.LittleEndian, &repSize); err != nil {
		return nil, errors.Wrap(err, "could not read reply size")
	}

	reply := make([]byte, repSize)
	reply, err := ioutil.ReadAll(io.LimitReader(r.stdout, int64(repSize)))
	if err != nil {
		return nil, errors.Wrap(err, "could not read reply body")
	}

	return reply, nil
}

func toReceiverError(reply []byte) error {
	res := make(map[string]string)
	if err := json.Unmarshal(reply, &res); err != nil {
		return errors.Wrap(err, "could not decode reply")
	}

	if status, ok := res["status"]; ok {
		if status == "ok" {
			return nil
		}

		if reason, ok := res["reason"]; ok {
			return Error(reason)
		}

		return fmt.Errorf("invalid reply")
	}

	return fmt.Errorf("invalid reply")
}
