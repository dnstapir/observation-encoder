package api

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/dnstapir/observation-encoder/internal/common"
)

type Conf struct {
	Active  bool   `json:"active"`
	Address string `json:"address"`
	Port    string `json:"port"`
	Log     common.Logger
	App     appHandle
	Certs   certHandle
}

type apiHandle struct {
	active          bool
	id              string
	log             common.Logger
	listenInterface string
	app             appHandle
	certs           certHandle
	srv             http.Server
}

type appHandle interface {
	GetPingCount() int64
}

type certHandle interface {
	GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
}

func Create(conf Conf) (*apiHandle, error) {
	a := new(apiHandle)

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	if conf.Certs == nil {
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
	a.certs = conf.Certs
	a.listenInterface = net.JoinHostPort(conf.Address, conf.Port)
	a.active = conf.Active
	a.id = "api"

	return a, nil
}

func (a *apiHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	if !a.active {
		exitCh <- common.Exit{ID: a.id, Err: nil}
		return
	}

	http.HandleFunc("/api/ping", a.apiHandlePing)

	cfg := &tls.Config{GetCertificate: a.certs.GetCertificate}
	srv := &http.Server{
		TLSConfig:    cfg,
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

func (a *apiHandle) apiHandlePing(rw http.ResponseWriter, r *http.Request) {
	n := a.app.GetPingCount()

	rw.Header().Set("Content-Type", "application/json; charset=utf-8")

	msg := fmt.Sprintf("{\"pings\": %d}", n)
	rw.Write([]byte(msg))
}
