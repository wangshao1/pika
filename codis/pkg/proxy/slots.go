// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"context"
	"sync"
	"time"

	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/utils/log"
)

type Slot struct {
	id   int
	lock struct {
		hold bool
		sync.RWMutex
	}
	refs sync.WaitGroup

	switched bool

	backend, migrate struct {
		id int
		bc *sharedBackendConn
	}
	replicaGroups [][]*sharedBackendConn

	method forwardMethod
}

func (s *Slot) snapshot() *models.Slot {
	var m = &models.Slot{
		Id:     s.id,
		Locked: s.lock.hold,

		BackendAddr:        s.backend.bc.Addr(),
		BackendAddrGroupId: s.backend.id,
		MigrateFrom:        s.migrate.bc.Addr(),
		MigrateFromGroupId: s.migrate.id,
		ForwardMethod:      s.method.GetId(),
	}
	for i := range s.replicaGroups {
		var group []string
		for _, bc := range s.replicaGroups[i] {
			group = append(group, bc.Addr())
		}
		m.ReplicaGroups = append(m.ReplicaGroups, group)
	}
	return m
}

func (s *Slot) blockAndWait() {
	if !s.lock.hold {
		s.lock.hold = true
		s.lock.Lock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		s.refs.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-ctx.Done():
		log.Warnf("slot %d has waited for 3 seconds, force kill backend connection", s.id)
		s.backend.bc.BlockAndClose()
		s.migrate.bc.BlockAndClose()
		s.refs.Wait()
	}
}

func (s *Slot) unblock() {
	if !s.lock.hold {
		return
	}
	s.lock.hold = false
	s.lock.Unlock()
}

func (s *Slot) forward(r *Request, hkey []byte) error {
	return s.method.Forward(s, r, hkey)
}
