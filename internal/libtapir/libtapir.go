package libtapir

import (
	"encoding/json"
	"time"

	"github.com/dnstapir/tapir"

	"github.com/dnstapir/observation-encoder/internal/common"
)

type Conf struct {
	Log   common.Logger
	Debug bool `toml:"debug"`
}

type libtapir struct {
	log common.Logger
}

func Create(conf Conf) (*libtapir, error) {
	lt := new(libtapir)
	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	lt.log = conf.Log

	return lt, nil
}

func (lt *libtapir) GenerateObservationMsg(domainStr string, flags uint32, ttl int) (string, error) {
	domain := tapir.Domain{
		Name:         domainStr,
		TimeAdded:    time.Now(),
		TTL:          ttl,
		TagMask:      tapir.TagMask(flags),
		ExtendedTags: []string{},
	}

	tapirMsg := tapir.TapirMsg{
		SrcName:   "dns-tapir",
		Creator:   "observation-encoder",
		MsgType:   "observation",
		ListType:  "doubtlist",
		Added:     []tapir.Domain{domain},
		Removed:   []tapir.Domain{},
		Msg:       "",
		TimeStamp: time.Now(),
		TimeStr:   "",
	}

	outMsg, err := json.Marshal(tapirMsg)
	if err != nil {
		lt.log.Error("Error serializing message, discarding...")
		return "", err
	}

	return string(outMsg), nil
}
