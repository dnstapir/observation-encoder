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

	"github.com/pelletier/go-toml/v2"

	"github.com/dnstapir/observation-encoder/internal/api"
	"github.com/dnstapir/observation-encoder/internal/app"
	"github.com/dnstapir/observation-encoder/internal/cert"
	"github.com/dnstapir/observation-encoder/internal/common"
	"github.com/dnstapir/observation-encoder/internal/libtapir"
	"github.com/dnstapir/observation-encoder/internal/logger"
	"github.com/dnstapir/observation-encoder/internal/nats"
)

const c_ENVVAR_OVERRIDE_NATS_URL = "DNSTAPIR_NATS_URL"

var commit = "BAD-BUILD"

type conf struct {
	app.Conf
	Debug    bool          `toml:"debug"`
	Api      api.Conf      `toml:"api"`
	Cert     cert.Conf     `toml:"cert"`
	Nats     nats.Conf     `toml:"nats"`
	Libtapir libtapir.Conf `toml:"libtapir"`
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

	debugFlag = debugFlag || mainConf.Debug

	/*
	 ******************************************************************
	 ********************** SET UP NATS *******************************
	 ******************************************************************
	 */
	natslog, err := logger.Create(
		logger.Conf{
			Debug: debugFlag || mainConf.Nats.Debug,
		})
	if err != nil {
		log.Error("Error creating nats log: %s", err)
	}

	envNatsUrl, overrideNatsUrl := os.LookupEnv(c_ENVVAR_OVERRIDE_NATS_URL)
	if overrideNatsUrl {
		mainConf.Nats.Url = envNatsUrl
		log.Info("Overriding NATS url with environment variable '%s'", c_ENVVAR_OVERRIDE_NATS_URL)
	}

	mainConf.Nats.Log = natslog
	natsHandle, err := nats.Create(mainConf.Nats)
	if err != nil {
		log.Error("Could not create NATS handle: %s", err)
		os.Exit(-1)
	}

	/*
	 ******************************************************************
	 ********************** SET UP LIBTAPIR ***************************
	 ******************************************************************
	 */
	libtapirlog, err := logger.Create(
		logger.Conf{
			Debug: debugFlag || mainConf.Libtapir.Debug,
		})
	if err != nil {
		log.Error("Error creating libtapir log: %s", err)
	}

	mainConf.Libtapir.Log = libtapirlog
	libtapirHandle, err := libtapir.Create(mainConf.Libtapir)
	if err != nil {
		log.Error("Could not create libtapir handle: %s", err)
		os.Exit(-1)
	}

	/*
	 ******************************************************************
	 ********************** SET UP MAIN APP ***************************
	 ******************************************************************
	 */
	applog, err := logger.Create(
		logger.Conf{
			Debug: debugFlag || mainConf.Debug,
		})
	if err != nil {
		log.Error("Error creating app log: %s", err)
	}

	mainConf.Log = applog
	mainConf.NatsHandle = natsHandle
	mainConf.LibtapirHandle = libtapirHandle
	appHandle, err := app.Create(mainConf.Conf)
	if err != nil {
		log.Error("Error creating application: '%s'", err)
		os.Exit(-1)
	}

	/*
	 ******************************************************************
	 ********************** SET UP CERT HANDLER ***********************
	 ******************************************************************
	 */
	certlog, err := logger.Create(
		logger.Conf{
			Debug: debugFlag || mainConf.Cert.Debug,
		})
	if err != nil {
		log.Error("Error creating cert log: %s", err)
	}

	mainConf.Cert.Log = certlog
	certHandle, err := cert.Create(mainConf.Cert)
	if err != nil {
		log.Error("Error creating cert manager: '%s'", err)
		os.Exit(-1)
	}

	/*
	 ******************************************************************
	 ********************** SET UP API ********************************
	 ******************************************************************
	 */
	apilog, err := logger.Create(
		logger.Conf{
			Debug: debugFlag || mainConf.Api.Debug,
		})
	if err != nil {
		log.Error("Error creating API log: %s", err)
	}
	mainConf.Api.Log = apilog
	mainConf.Api.App = appHandle
	mainConf.Api.Certs = certHandle
	apiHandle, err := api.Create(mainConf.Api)
	if err != nil {
		log.Error("Error creating API: '%s'", err)
		os.Exit(-1)
	}

	/*
	 ******************************************************************
	 ********************** START RUNNING STUFF ***********************
	 ******************************************************************
	 */
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
