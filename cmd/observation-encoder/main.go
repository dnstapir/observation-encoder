package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	//"github.com/nats-io/nats.go"
	"github.com/pelletier/go-toml/v2"

	"github.com/dnstapir/observation-encoder/internal/api"
	"github.com/dnstapir/observation-encoder/internal/app"
	"github.com/dnstapir/observation-encoder/internal/cert"
	"github.com/dnstapir/observation-encoder/internal/common"
	"github.com/dnstapir/observation-encoder/internal/logger"
)

var commit = "BAD-BUILD"

type conf struct {
	app.Conf
	Debug bool      `toml:"debug"`
	Api   api.Conf  `toml:"api"`
	Cert  cert.Conf `toml:"cert"`
}

func main() {
	var configFile string
	var runVersionCmd bool
	var debugFlag bool
	var mainConf conf

	flag.BoolVar(&runVersionCmd,
		"version",
		false,
		"Print version then exit",
	)
	flag.StringVar(&configFile,
		"config",
		"config.toml",
		"Configuration file to use",
	)
	flag.BoolVar(&debugFlag,
		"debug",
		false,
		"Enable DEBUG logs",
	)
	flag.Parse()

	log, err := logger.Create(
		logger.Conf{
			Debug: debugFlag,
		})
	if err != nil {
		panic(fmt.Sprintf("Could not create logger, err: '%s'", err))
	}

	log.Info("observation-encoder, commit: '%s'", commit)
	if runVersionCmd {
		os.Exit(0)
	}

	if configFile == "" {
		log.Error("No config file specified, exiting...")
		os.Exit(-1)
	}

	file, err := os.Open(configFile)
	if err != nil {
		log.Error("Couldn't open config file '%s', exiting...", configFile)
		os.Exit(-1)
	}
	defer file.Close()

	confDecoder := toml.NewDecoder(file)
	if confDecoder == nil {
		log.Error("Problem decoding config file '%s', exiting...", configFile)
		os.Exit(-1)
	}

	confDecoder.DisallowUnknownFields()
	confDecoder.Decode(&mainConf)
	file.Close() // TODO okay to close here while also using defer above?

	// TODO create different loggers with different debug settings
	applog, err := logger.Create(
		logger.Conf{
			Debug: debugFlag || mainConf.Debug,
		})
	if err != nil {
		log.Error("Error creating app log: %s", err)
	} else {
		applog.Debug("Debug logging enabled")
	}

	mainConf.Log = applog
	appHandle, err := app.Create(mainConf.Conf)
	if err != nil {
		log.Error("Error creating application: '%s'", err)
		os.Exit(-1)
	}

	mainConf.Cert.Log = applog
	certHandle, err := cert.Create(mainConf.Cert)
	if err != nil {
		log.Error("Error creating cert manager: '%s'", err)
		os.Exit(-1)
	}

	mainConf.Api.Log = applog
	mainConf.Api.App = appHandle
	mainConf.Api.Certs = certHandle
	apiHandle, err := api.Create(mainConf.Api)
	if err != nil {
		log.Error("Error creating API: '%s'", err)
		os.Exit(-1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer close(sigChan)
	defer signal.Stop(sigChan)

	ctx, cancel := context.WithCancel(context.Background())
	exitCh := make(chan common.Exit)

	log.Info("Starting threads...")

	go appHandle.Run(ctx, exitCh)

	go apiHandle.Run(ctx, exitCh)
	go certHandle.Run(ctx, exitCh)

	log.Info("Threads started!")

	exitLoop := false
	var wg sync.WaitGroup
	wg.Add(1)
	for {
		select {
		case s, ok := <-sigChan:
			if ok {
				log.Info("Got signal '%s'", s)
				exitLoop = true
			} else {
				log.Info("signal channel was closed")
				sigChan = nil
			}
		case exit, ok := <-exitCh:
			if ok {
				if exit.Err != nil {
					log.Error("%s exited with error: '%s'", exit.ID, exit.Err)
					if exit.Err == common.ErrFatal {
						exitLoop = true
					}
				} else {
					log.Info("%s done!", exit.ID)
				}
			} else {
				log.Warning("exit channel closed unexpectedly")
				exitCh = nil
			}
		}
		if exitLoop || (sigChan == nil && exitCh == nil) {
			log.Info("Leaving toplevel loop")
			wg.Done()
			break
		}
	}

	log.Info("Cancelling, giving threads some time to finish...")
	cancel()
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(2 * time.Second)
		timeout <- true
	}()

TIMEOUT_LOOP:
	for {
		select {
		case exit, ok := <-exitCh:
			if ok {
				if exit.Err != nil {
					log.Error("%s exited with error: '%s'", exit.ID, exit.Err)
				} else {
					log.Info("%s done!", exit.ID)
				}
			} else {
				log.Info("exit channel was closed during shutdown")
				exitCh = nil
			}
		case <-timeout:
			log.Debug("Time's up. Proceeding with shutdown.")
			break TIMEOUT_LOOP
		}
		if exitCh == nil {
			log.Warning("exit channel closed unexpectedly")
			break TIMEOUT_LOOP
		}
	}

	close(exitCh)
	wg.Wait()
	log.Info("Exiting...")
	os.Exit(0)
}
