package app

import (
	"context"
    "slices"
    "strings"
	"sync/atomic"

	"github.com/dnstapir/observation-encoder/internal/common"
)

const c_N_HANDLERS = 3

type Conf struct {
	Log     common.Logger
	Debug   bool `toml:"debug"`
	Address string `toml:"address"`
	Port    string `toml:"port"`
    SubjectPrefix string `toml:"subject_prefix"`
    NatsHandle    nats
}

type appHandle struct {
	id      string
	log     common.Logger
    natsHandle nats
	address string
	port    string
	subjectPrefix    string
	exitCh  chan<- common.Exit
	pm
}

type pm struct {
	natsInCount atomic.Int64
}

type job struct {
    msg common.NatsMsg
}

type nats interface {
	WatchBucket(context.Context, string) (<-chan common.NatsMsg, error)
    GetObservations(context.Context, string) (uint32, error)
    Shutdown() error
}

func Create(conf Conf) (*appHandle, error) {
	a := new(appHandle)

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	if conf.NatsHandle == nil {
		return nil, common.ErrBadHandle
	}

	if conf.Address == "" {
		return nil, common.ErrBadParam
	}

	if conf.Port == "" {
		return nil, common.ErrBadParam
	}

	if conf.SubjectPrefix == "" {
        // TODO make sure is valid NATS subject
		return nil, common.ErrBadParam
	}

	a.log = conf.Log
	a.address = conf.Address
	a.port = conf.Port
	a.id = "main app"
    a.natsHandle = conf.NatsHandle
    a.subjectPrefix = conf.SubjectPrefix

	return a, nil
}

func (a *appHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	a.id = "main app"
	a.exitCh = exitCh
	jobChan := make(chan job, 10)

    natsInCh, err := a.natsHandle.WatchBucket(ctx, a.subjectPrefix)
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
		case msg := <-natsInCh:
		    a.pm.natsInCount.Add(1)
            j := job {
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

    domainRev, ok := strings.CutPrefix(j.msg.Subject, a.subjectPrefix)
    if !ok {
        a.log.Warning("Prefix mismatch for incoming msg subject, dropping...")
        return
    }
    domainSplit := strings.Split(domainRev, ".")
    // TODO check split is not too short
    slices.Reverse(domainSplit)

    domain := strings.Join(domainSplit[:len(domainSplit)-2], ".")

    a.log.Debug("Extracted domain '%s'", domain)

    obs, err := a.natsHandle.GetObservations(ctx, domain)
    if err != nil {
        a.log.Error("Could not get observations for %s: %s", domain, err)
        return
    }

    a.log.Debug("%s has observation vector %d", domain, obs)

	return
}

func (a *appHandle) GetNatsInCount() int64 {
	return a.pm.natsInCount.Load()
}
