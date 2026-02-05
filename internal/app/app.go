package app

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/dnstapir/observation-encoder/internal/common"
)

const c_N_HANDLERS = 3

type Conf struct {
	Log     common.Logger
	Address string `json:"address"`
	Port    string `json:"port"`
}

type appHandle struct {
	id      string
	log     common.Logger
	address string
	port    string
	exitCh  chan<- common.Exit
	pm
}

type pm struct {
	pingCount atomic.Int64
}

type job struct {
	conn net.Conn
}

func Create(conf Conf) (*appHandle, error) {
	a := new(appHandle)

	if conf.Log == nil {
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

	return a, nil
}

func (a *appHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	a.id = "main app"
	a.exitCh = exitCh
	jobChan := make(chan job, 10)

	for range c_N_HANDLERS {
		go func() {
			for j := range jobChan {
				a.handleJob(j)
			}
		}()
	}

	l, err := net.Listen("tcp", net.JoinHostPort(a.address, a.port))
	if err != nil {
		a.log.Error("Error creating tcp listener: '%s'", err)
		a.exitCh <- common.Exit{ID: a.id, Err: common.ErrFatal}
		return
	}

	a.log.Info("Starting tcp listener loop")
	go func() {
		for {
			conn, err := l.Accept()
			if errors.Is(err, net.ErrClosed) {
				a.log.Info("Socket closed, listener terminating")
				break
			} else if err != nil {
				a.log.Error("Failed to accept connection '%s'", err)
				continue
			}

			jobChan <- job{conn: conn}
			a.log.Debug("Connection passed to handler")
		}
	}()

	<-ctx.Done()
	a.log.Info("Stopping listener thread")
	l.Close()

	for len(jobChan) > 0 {
		j := <-jobChan
		j.conn.Close()
	}
	close(jobChan)

	a.exitCh <- common.Exit{ID: a.id, Err: nil}
	a.log.Info("Main app shutdown done")
	return
}

func (a *appHandle) handleJob(j job) {
	c := j.conn
	defer c.Close()

	bufsize := 100
	buf := make([]byte, bufsize) // TODO make configurable
	n, err := c.Read(buf)
	if err != nil {
		a.log.Error("Error handling connection, closing")
		return
	}

	a.log.Debug("Received data '%s'", string(buf[:n]))

	msg := string(buf[:n])
	if msg == "ping\r\n" {

		a.pm.pingCount.Add(1)
		_, err := c.Write([]byte("pong\r\n"))
		if err != nil {
			a.log.Error("Error responding to ping, closing")
			return
		}
	}

	return
}

func (a *appHandle) GetPingCount() int64 {
	return a.pm.pingCount.Load()
}
