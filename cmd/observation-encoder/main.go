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

const env_DNSTAPIR_NATS_URL = "DNSTAPIR_NATS_URL"

/* Rewritten if building with make */
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
		log.Error("Problem creating decoder for config file '%s', exiting...", configFile)
		os.Exit(-1)
	}

	confDecoder.DisallowUnknownFields()
	err = confDecoder.Decode(&mainConf)
	if err != nil {
		log.Error("Problem decoding config file '%s': %s", configFile, err)
		os.Exit(-1)
	}

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
		os.Exit(-1)
	}

	envNatsUrl, overrideNatsUrl := os.LookupEnv(env_DNSTAPIR_NATS_URL)
	if overrideNatsUrl {
		mainConf.Nats.Url = envNatsUrl
		log.Info("Overriding NATS url with environment variable '%s'", env_DNSTAPIR_NATS_URL)
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
		os.Exit(-1)
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
		os.Exit(-1)
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
		os.Exit(-1)
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
		os.Exit(-1)
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
	exitCh := make(chan common.Exit, 10) // TODO adjustable buffer?

	log.Info("Starting threads...")

	var wg sync.WaitGroup
	wg.Go(func() { appHandle.Run(ctx, exitCh) })
	wg.Go(func() { apiHandle.Run(ctx, exitCh) })
	wg.Go(func() { certHandle.Run(ctx, exitCh) })

	log.Info("Threads started!")

	exitLoop := false
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
			break
		}
	}

	log.Info("Cancelling threads")
	cancel()

	log.Info("Waiting for threads to finish")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	const shutdownTimeout = 30 * time.Second
	select {
	case <-done:
		log.Info("Threads finished")
	case <-time.After(shutdownTimeout):
		log.Error("Timed out waiting for threads to finish after %s", shutdownTimeout)
	}

	log.Info("Exiting...")
	os.Exit(0)
}
