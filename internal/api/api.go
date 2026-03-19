package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/dnstapir/tapir-analyse-lib/common"
)

type Conf struct {
	Active  bool   `toml:"active"`
	Debug   bool   `toml:"debug"`
	Address string `toml:"address"`
	Port    string `toml:"port"`
	Log     common.Logger
	App     appHandle
}

type apiHandle struct {
	active          bool
	id              string
	log             common.Logger
	listenInterface string
	app             appHandle
	srv             http.Server
}

type appHandle interface {
	GetNatsInCount() int64
}

func Create(conf Conf) (*apiHandle, error) {
	a := new(apiHandle)
	a.id = "api"
	a.active = conf.Active

	if !a.active {
		return a, nil
	}

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	if conf.App == nil {
		return nil, common.ErrBadHandle
	}

	if conf.Address == "" {
		return nil, common.ErrBadParam
	}

	if conf.Port == "" {
		return nil, common.ErrBadParam
	}

	a.log = conf.Log
	a.app = conf.App
	a.listenInterface = net.JoinHostPort(conf.Address, conf.Port)
	a.active = conf.Active

	return a, nil
}

func (a *apiHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	if !a.active {
		exitCh <- common.Exit{ID: a.id, Err: nil}
		return
	}

	http.HandleFunc("/api/nats_in", a.apiHandleNatsIn)

	srv := &http.Server{
		Addr:         a.listenInterface,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}

	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = srv.ListenAndServeTLS("", "")
		if errors.Is(err, http.ErrServerClosed) {
			a.log.Info("API server closing")
			err = nil
		} else {
			a.log.Error("Unexpected API server shutdown: '%s'", err)
		}

	}()

	<-ctx.Done()
	a.log.Info("Shutting down API")
	shutdownCtx, _ := context.WithTimeout(context.Background(), time.Second*2)
	srv.Shutdown(shutdownCtx)
	wg.Wait()

	exitCh <- common.Exit{ID: a.id, Err: err}
	a.log.Info("API server shutdown done")
	return
}

func (a *apiHandle) apiHandleNatsIn(rw http.ResponseWriter, r *http.Request) {
	n := a.app.GetNatsInCount()

	rw.Header().Set("Content-Type", "application/json; charset=utf-8")

	msg := fmt.Sprintf("{\"nats_in\": %d}", n)
	rw.Write([]byte(msg))
}
