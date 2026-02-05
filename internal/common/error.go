package common

import "errors"

var ErrFatal = errors.New("fatal")
var ErrBadHandle = errors.New("bad handle")
var ErrBadParam = errors.New("bad parameter")
