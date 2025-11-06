package mq

import "crypto/tls"

// BuildTLSConfig returns a tls.Config based on the supplied connection configuration.
// When cfg.TLSConfig is provided, it is cloned and returned. Otherwise the function
// returns a minimal TLS configuration when UseTLS is true. If TLS is not requested,
// nil is returned.
func BuildTLSConfig(cfg Config) (*tls.Config, error) {
	if cfg.TLSConfig != nil {
		return cfg.TLSConfig.Clone(), nil
	}

	if !cfg.UseTLS {
		return nil, nil
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12,
	}, nil
}
