package common

import "errors"

var ErrFatal = errors.New("fatal")
var ErrBadHandle = errors.New("bad handle")
var ErrBadParam = errors.New("bad parameter")
var ErrBadFlag = errors.New("bad observation flag")
var ErrBadKey = errors.New("bad nats key")
