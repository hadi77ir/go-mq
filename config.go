package mq

import "crypto/tls"

// Config holds common connection parameters that are shared across broker implementations.
type Config struct {
	// Addresses lists broker endpoints in host:port form.
	Addresses []string

	// Username is used for brokers that require authentication.
	Username string

	// Password is the password credential for authentication.
	Password string

	// VirtualHost represents an optional namespace or virtual host.
	VirtualHost string

	// UseTLS enables TLS when communicating with the broker.
	UseTLS bool

	// TLSConfig allows callers to supply a fully configured TLS stack.
	// When set it takes precedence over UseTLS.
	TLSConfig *tls.Config
}
