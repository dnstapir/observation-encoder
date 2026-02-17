package app

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/dnstapir/observation-encoder/internal/common"
)

const c_N_HANDLERS = 3
const c_NATS_DELIM = common.NATS_DELIM

type Conf struct {
	Log            common.Logger
	Debug          bool `toml:"debug"`
	NatsHandle     nats
	LibtapirHandle libtapir
}

type appHandle struct {
	id             string
	log            common.Logger
	natsHandle     nats
	libtapirHandle libtapir
	exitCh         chan<- common.Exit
	pm
}

type pm struct {
	natsInCount atomic.Int64
}

type job struct {
	msg common.NatsMsg
}

type nats interface {
	WatchObservations(context.Context) (<-chan common.NatsMsg, error)
	RemovePrefix(string) string
	GetObservations(context.Context, string) (uint32, error)
	SendSouthboundObservation(string) error
	Shutdown() error
}

type libtapir interface {
	GenerateObservationMsg(string, uint32) (string, error) // TODO set ttl?
}

func Create(conf Conf) (*appHandle, error) {
	a := new(appHandle)

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	if conf.NatsHandle == nil {
		return nil, common.ErrBadHandle
	}

	if conf.LibtapirHandle == nil {
		return nil, common.ErrBadHandle
	}

	a.log = conf.Log
	a.id = "main app"
	a.natsHandle = conf.NatsHandle
	a.libtapirHandle = conf.LibtapirHandle

	return a, nil
}

func (a *appHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	a.id = "main app"
	a.exitCh = exitCh
	jobChan := make(chan job, 10)

	natsInCh, err := a.natsHandle.WatchObservations(ctx)
	if err != nil {
		a.log.Error("Error connecting to NATS: %s", err)
		a.exitCh <- common.Exit{ID: a.id, Err: err}
		return
	}

	for range c_N_HANDLERS {
		go func() {
			for j := range jobChan {
				a.handleJob(ctx, j)
			}
		}()
	}

MAIN_APP_LOOP:
	for {
		select {
		case msg, ok := <-natsInCh:
			if !ok {
				a.log.Warning("NATS channel closed, exiting main loop")
				break MAIN_APP_LOOP
			}
			a.pm.natsInCount.Add(1)
			j := job{
				msg: msg,
			}
			jobChan <- j
		case <-ctx.Done():
			a.log.Info("Stopping main worker thread")
			break MAIN_APP_LOOP
		}
	}

	for len(jobChan) > 0 {
		<-jobChan
	}
	close(jobChan)

	err = a.natsHandle.Shutdown()
	if err != nil {
		a.log.Error("Encountered '%s' during NATS shutdown", err)
	}

	a.exitCh <- common.Exit{ID: a.id, Err: err}
	a.log.Info("Main app shutdown done")
	return
}

func (a *appHandle) handleJob(ctx context.Context, j job) {
	a.log.Info("Got message on subject '%s'", j.msg.Subject)

	domainRev := a.natsHandle.RemovePrefix(j.msg.Subject)
	domainSplit := strings.Split(domainRev, c_NATS_DELIM)

	if len(domainSplit) < 2 { /* at least one observation type and one DNS label */
		a.log.Warning("Incoming message subject %s has too few labels, ignoring...", j.msg.Subject)
		return
	}

	slices.Reverse(domainSplit)

	/* After reverse, observation type is at the end. Drop it, we just want the domain name */
	domain := strings.Join(domainSplit[:len(domainSplit)-1], c_NATS_DELIM)

	a.log.Debug("Extracted domain '%s'", domain)

	obs, err := a.natsHandle.GetObservations(ctx, domain)
	if err != nil {
		a.log.Error("Could not get observations for %s: %s", domain, err)
		return
	}

	a.log.Debug("%s has observation vector %d", domain, obs)

	obsJSON, err := a.libtapirHandle.GenerateObservationMsg(domain, obs)
	if err != nil {
		a.log.Error("Couldn't generate JSON observation: %s", err)
		return
	}

	err = a.natsHandle.SendSouthboundObservation(obsJSON)
	if err != nil {
		a.log.Error("Couldn't send southbound observation: %s", err)
	}

	a.log.Debug("Done handling msg on subject %s", j.msg.Subject)

	return
}

func (a *appHandle) GetNatsInCount() int64 {
	return a.pm.natsInCount.Load()
}
