package nats

import (
	"testing"

	"github.com/dnstapir/tapir-analyse-lib/common"
	"github.com/dnstapir/tapir-analyse-lib/logger"
)

var log = logger.New(
	logger.Conf{
		Debug: false,
	})

func TestGenKeyFilterSubject(t *testing.T) {
	tests := map[string]struct {
		input1 string
		input2 string
		expect string
	}{
		"1-prefix, 2-domain": {
			input1: "obs",
			input2: "foo.xa",
			expect: "obs.*.xa.foo",
		},
		"2-prefix, 3-domain": {
			input1: "obs1.obs2",
			input2: "www.foo.xa",
			expect: "obs1.obs2.*.xa.foo.www",
		},
		"2-prefix, 3-domain, fqdn": {
			input1: "obs1.obs2",
			input2: "www.foo.xa.",
			expect: "obs1.obs2.*.xa.foo.www",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			nh := natsClient{
				log:                      log,
				observationSubjectPrefix: test.input1,
			}

			got := nh.genKeyFilterSubject(test.input2)

			if got != test.expect {
				t.Fatalf("Got: '%s', Expected: '%s'", got, test.expect)
			}
		})
	}
}

func TestRemovePrefix(t *testing.T) {
	tests := map[string]struct {
		input1 string
		input2 string
		expect string
	}{
		"one label prefix": {
			input1: "obs",
			input2: "obs.foo",
			expect: "foo",
		},
		"two label prefix": {
			input1: "obs1.obs2",
			input2: "obs1.obs2.foo",
			expect: "foo",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			nh := natsClient{
				log:                      log,
				observationSubjectPrefix: test.input1,
			}

			got := nh.RemovePrefix(test.input2)

			if got != test.expect {
				t.Fatalf("Got: '%s', Expected: '%s'", got, test.expect)
			}
		})
	}
}

func TestExtractObservationFromKey(t *testing.T) {
	tests := map[string]struct {
		input1    string
		input2    string
		expect    uint32
		expectErr error
	}{
		"globally_new, one label prefix": {
			input1:    "obs",
			input2:    "obs.globally_new.xa.foo",
			expect:    1,
			expectErr: nil,
		},
		"globally_new, two label prefix": {
			input1:    "obs1.obs2",
			input2:    "obs1.obs2.globally_new.xa.foo",
			expect:    1,
			expectErr: nil,
		},
		"bad flag": {
			input1:    "obs1.obs2",
			input2:    "obs1.obs2.bad_flag.xa.foo",
			expect:    0,
			expectErr: common.ErrBadFlag,
		},
		"too short key": {
			input1:    "obs1.obs2",
			input2:    "obs1.obs2.looptest",
			expect:    0,
			expectErr: common.ErrBadKey,
		},
		"single-label key": {
			input1:    "obs",
			input2:    "obs.looptest.xa",
			expect:    1024,
			expectErr: nil,
		},
		"long prefix, short domain name": {
			input1:    "a.b.c.d.e.f.g.h.j.k",
			input2:    "a.b.c.d.e.f.g.h.j.k.globally_new.xa",
			expect:    1,
			expectErr: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			nh := natsClient{
				log:                      log,
				observationSubjectPrefix: test.input1,
			}

			got, err := nh.extractObservationFromKey(test.input2)
			if err != test.expectErr {
				t.Fatalf("Got: '%s', Expected: '%s'", err, test.expectErr)
			}

			if got != test.expect {
				t.Fatalf("Got: '%d', Expected: '%d'", got, test.expect)
			}
		})
	}
}
