package cert

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dnstapir/observation-encoder/internal/common"
)

type Conf struct {
	Log      common.Logger
    Debug    bool   `toml:"debug"`
	Interval int    `toml:"interval"`
	CertDir  string `toml:"cert_dir"`
}

type certHandle struct {
	sync.RWMutex

	id      string
	log     common.Logger
	ticker  *time.Ticker
	certDir string
	certs   map[string]*tls.Certificate
}

func Create(conf Conf) (*certHandle, error) {
	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	c := new(certHandle)
	c.id = "cert manager"
	c.log = conf.Log

	if conf.CertDir == "" {
		return nil, common.ErrBadParam
	}

	c.certDir = path.Clean(conf.CertDir)
	if conf.Interval > 0 {
		c.ticker = time.NewTicker(time.Duration(conf.Interval) * time.Second)
	} else {
		c.ticker = time.NewTicker(time.Duration(math.MaxInt32) * time.Second)
		c.ticker.Stop()
		c.log.Warning("No interval set for scanning cert directory. Won't be refreshing.")
	}

	c.certs = make(map[string]*tls.Certificate)

	err := c.scanCertDir(context.Background())
	if err != nil {
		c.log.Error("First time scanning cert dir failed: %s", err)
		return nil, err
	}

	return c, nil
}

func (c *certHandle) GetCertificate(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
	keys := make([]string, 0)
	c.RLock()
	defer c.RUnlock()

	for k := range c.certs {
		keys = append(keys, k)
	}

	if len(keys) == 0 {
		return nil, errors.New("no certificates configured")
	} else if len(keys) == 1 || info.ServerName == "" {
		return c.certs[keys[0]], nil
	}

	cert, ok := c.certs[info.ServerName]
	if ok {
		c.log.Debug("Found direct certificate match for %s", info.ServerName)
		return cert, nil
	}

	nsplit := strings.Split(info.ServerName, ".")
	nsplit[0] = "*"
	wildcard := strings.Join(nsplit, ".") + "." // TODO double-check wildcard domains
	c.log.Debug("Checking if wildcard certificate %s is present", wildcard)

	cert, ok = c.certs[wildcard]
	if ok {
		c.log.Debug("Wildcard certificate match for %s", info.ServerName)
		return cert, nil
	}

	c.log.Debug("No certificate match for %s, falling back to using %s", info.ServerName, keys[0])
	return c.certs[keys[0]], nil
}

func (c *certHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	defer c.ticker.Stop()

CERT_LOOP:
	for {
		select {
		case <-ctx.Done():
			break CERT_LOOP
		case <-c.ticker.C:
			err := c.scanCertDir(ctx)
			if err != nil {
				if err == common.ErrFatal {
					exitCh <- common.Exit{ID: c.id, Err: err}
					return
				} else {
					c.log.Error("Failed scanning cert directory: %s", err)
				}
			} else {
				c.log.Debug("Re-scan of cert dir done!")
			}
		}
	}

	exitCh <- common.Exit{ID: c.id, Err: nil}
	c.log.Info("Cert handler shutdown done")
	return
}

func (c *certHandle) scanCertDir(ctx context.Context) error {
	files, err := os.ReadDir(c.certDir)
	if err != nil {
		c.log.Error("Failed to read certificate directory: %s", err)
	}

	foundCerts := make(map[string]byte)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		certname, cfound := strings.CutSuffix(file.Name(), "crt.pem")
		if cfound {
			foundCerts[certname] |= 0b00000001
		}
		keyname, kfound := strings.CutSuffix(file.Name(), "key.pem")
		if kfound {
			foundCerts[keyname] |= 0b00000010
		}
	}

	newCerts := make(map[string]*tls.Certificate)
	for k, v := range foundCerts {
		if v != 0b00000011 {
			continue
		}

		certfile := filepath.Join(c.certDir, k+"crt.pem")
		keyfile := filepath.Join(c.certDir, k+"key.pem")

		c.log.Debug("Attempting to load certificate '%s'", certfile)

		cert, err := tls.LoadX509KeyPair(certfile, keyfile)
		if err != nil {
			c.log.Error("Failed to read certificate '%s' or its key: %s", certfile, err)
			continue
		}

		if cert.Leaf != nil {
			c.log.Debug("Found leaf certificate")
			cn, _ := strings.CutSuffix(cert.Leaf.Subject.CommonName, ".")
			cnFqdn := cn + "." // TODO really include these dotted ones?
			newCerts[cn] = &cert
			newCerts[cnFqdn] = &cert
			for _, dnsname := range cert.Leaf.DNSNames {
				c.log.Debug("Found DNS Name %s in cert", dnsname)
				san, _ := strings.CutSuffix(dnsname, ".")
				sanFqdn := san + "." // TODO really include these dotted ones?
				newCerts[san] = &cert
				newCerts[sanFqdn] = &cert
			}
		}
	}

	c.Lock()
	defer c.Unlock()
	c.certs = newCerts

	return nil
}
