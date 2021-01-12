package backend

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// NamedLocks provides a thread-safe map of named locks, used for locking
// repositories during critical operations (commits, GC, etc.)
type NamedLocks struct {
	locks sync.Map
}

// WithLock runs the given task, locking the "name" mutex for the
// duration of the task
func (l *NamedLocks) WithLock(ctx context.Context, name string, task func() error) error {
	s := semaphore.NewWeighted(1)
	m, _ := l.locks.LoadOrStore(name, s)
	sem := m.(*semaphore.Weighted)
	sem.Acquire(ctx, 1)
	defer sem.Release(1)

	return task()
}

func (l *NamedLocks) IsLocked(name string) bool {
	m, ok := l.locks.Load(name)
	if !ok {
		return false
	}
	sem := m.(*semaphore.Weighted)
	couldAcquire := sem.TryAcquire(1)
	if couldAcquire {
		// we unlock after returning the value
		defer sem.Release(1)
		return false
	}
	return true
}
