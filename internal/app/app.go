package app

import (
	"context"
	"sync/atomic"

	"github.com/dnstapir/observation-encoder/internal/common"
)

const c_N_HANDLERS = 3

type Conf struct {
	Log     common.Logger
	Address string `toml:"address"`
	Port    string `toml:"port"`
    NatsHandle    nats
}

type appHandle struct {
	id      string
	log     common.Logger
    natsHandle nats
	address string
	port    string
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
	WatchBucket(context.Context) (<-chan common.NatsMsg, error)
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

	a.log = conf.Log
	a.address = conf.Address
	a.port = conf.Port
	a.id = "main app"
    a.natsHandle = conf.NatsHandle

	return a, nil
}

func (a *appHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	a.id = "main app"
	a.exitCh = exitCh
	jobChan := make(chan job, 10)

    natsInCh, err := a.natsHandle.WatchBucket(ctx)
    if err != nil {
        a.log.Error("Error connecting to NATS: %s", err)
	    a.exitCh <- common.Exit{ID: a.id, Err: err}
        return
    }

	for range c_N_HANDLERS {
		go func() {
			for j := range jobChan {
				a.handleJob(j)
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

func (a *appHandle) handleJob(j job) {
    a.log.Info("Got message on subject '%s'", j.msg.Subject)
    // TODO impl

	return
}

func (a *appHandle) GetNatsInCount() int64 {
	return a.pm.natsInCount.Load()
}
