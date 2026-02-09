package common

const NATSHEADER_KEY_IDENTIFIER = "DNSTAPIR-Key-Identifier"
const NATSHEADER_KEY_THUMBPRINT = "DNSTAPIR-Key-Thumbprint"

const NATS_WILDCARD = "*"
const NATS_GLOB = ">"
const NATS_DELIM = "."

var NATSHEADERS_DNSTAPIR_ALL = []string{
	NATSHEADER_KEY_IDENTIFIER,
	NATSHEADER_KEY_THUMBPRINT,
}

type NatsMsg struct {
	Headers map[string]string
	Subject string
	Data    []byte
}
