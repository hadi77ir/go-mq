package valkeymq

import (
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/hadi77ir/go-mq"
	valkey "github.com/valkey-io/valkey-go"
)

const (
	fieldPayload       = "payload" // CBOR-encoded message envelope
	fieldBody          = "body"    // Legacy field for backward compatibility
	fieldContentType   = "content_type"
	fieldTimestamp     = "timestamp"
	fieldKey           = "key"
	fieldReplyTo       = "reply_to"
	fieldCorrelationID = "correlation_id"
	fieldDelay         = "delay_ms"
	fieldExpiration    = "expiration_ms"
	fieldPersistent    = "persistent"
	fieldMandatory     = "mandatory"
	headerPrefix       = "header."
)

func encodeMessage(msg mq.Message, opts publishOptsInternal) map[string]string {
	// Use CBOR encoding similar to pub/sub mode for efficiency and binary support
	envelope := pubSubEnvelope{
		Key:         msg.Key,
		Body:        msg.Body,
		Headers:     msg.Headers,
		ContentType: msg.ContentType,
	}
	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	envelope.Timestamp = ts.UnixNano()

	if opts.ReplyTo != "" {
		envelope.ReplyTo = opts.ReplyTo
	}
	if opts.CorrelationID != "" {
		envelope.CorrelationID = opts.CorrelationID
	}
	// Merge opts.Headers into message headers
	if opts.Headers != nil {
		if envelope.Headers == nil {
			envelope.Headers = make(map[string]string)
		}
		for k, v := range opts.Headers {
			envelope.Headers[k] = v
		}
	}

	// CBOR-encode the entire envelope
	cborData, err := cbor.Marshal(envelope)
	if err != nil {
		// Fallback: use legacy format if CBOR encoding fails
		fields := make(map[string]string, len(msg.Headers)+10)
		if len(msg.Body) > 0 {
			// Legacy fallback: store body as raw string
			fields[fieldBody] = string(msg.Body)
		}
		if msg.ContentType != "" {
			fields[fieldContentType] = msg.ContentType
		}
		if !msg.Timestamp.IsZero() {
			fields[fieldTimestamp] = msg.Timestamp.UTC().Format(time.RFC3339Nano)
		} else {
			fields[fieldTimestamp] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if msg.Key != "" {
			fields[fieldKey] = msg.Key
		}
		for k, v := range msg.Headers {
			fields[headerPrefix+k] = v
		}
		return fields
	}

	// Store CBOR-encoded payload as a string field
	// Valkey Streams accept string values, and Go strings can hold arbitrary bytes (binary-safe)
	// This is more efficient than base64-encoding, as we store the CBOR bytes directly
	return map[string]string{
		fieldPayload: string(cborData), // Store CBOR bytes directly as string (Valkey accepts binary-safe strings)
	}
}

func decodeMessage(entry valkey.XRangeEntry) mq.Message {
	// Try to decode as CBOR payload first (new format)
	if payload, ok := entry.FieldValues[fieldPayload]; ok {
		var envelope pubSubEnvelope
		if err := cbor.Unmarshal([]byte(payload), &envelope); err == nil {
			ts := time.Time{}
			if envelope.Timestamp > 0 {
				ts = time.Unix(0, envelope.Timestamp).UTC()
			}
			return mq.Message{
				Key:         envelope.Key,
				Body:        envelope.Body,
				Headers:     envelope.Headers,
				ContentType: envelope.ContentType,
				Timestamp:   ts,
			}
		}
	}

	// Fallback to legacy format (backward compatibility)
	headers := make(map[string]string)
	var body []byte
	var contentType string
	var ts time.Time
	var key string

	for field, value := range entry.FieldValues {
		switch {
		case field == fieldBody:
			// Legacy: body was stored as raw string
			body = []byte(value)
		case field == fieldContentType:
			contentType = value
		case field == fieldTimestamp:
			if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
				ts = parsed
			}
		case field == fieldKey:
			key = value
		case len(field) > len(headerPrefix) && field[:len(headerPrefix)] == headerPrefix:
			headers[field[len(headerPrefix):]] = value
		default:
			headers[field] = value
		}
	}

	return mq.Message{
		Key:         key,
		Body:        body,
		Headers:     headers,
		ContentType: contentType,
		Timestamp:   ts,
	}
}

type pubSubEnvelope struct {
	Key           string            `cbor:"1,keyasint,omitempty"`
	Body          []byte            `cbor:"2,keyasint"`
	Headers       map[string]string `cbor:"3,keyasint,omitempty"`
	ContentType   string            `cbor:"4,keyasint,omitempty"`
	Timestamp     int64             `cbor:"5,keyasint,omitempty"` // Unix nanoseconds
	ReplyTo       string            `cbor:"6,keyasint,omitempty"`
	CorrelationID string            `cbor:"7,keyasint,omitempty"`
	Delay         int64             `cbor:"8,keyasint,omitempty"` // milliseconds
	Expiration    int64             `cbor:"9,keyasint,omitempty"` // milliseconds
	Persistent    bool              `cbor:"10,keyasint,omitempty"`
	Mandatory     bool              `cbor:"11,keyasint,omitempty"`
}

func encodePubSubPayload(msg mq.Message, opts publishOptsInternal) []byte {
	envelope := pubSubEnvelope{
		Key:         msg.Key,
		Body:        msg.Body,
		Headers:     msg.Headers,
		ContentType: msg.ContentType,
	}
	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	envelope.Timestamp = ts.UnixNano()

	if opts.ReplyTo != "" {
		envelope.ReplyTo = opts.ReplyTo
	}
	if opts.CorrelationID != "" {
		envelope.CorrelationID = opts.CorrelationID
	}
	// Merge opts.Headers into message headers
	if opts.Headers != nil {
		if envelope.Headers == nil {
			envelope.Headers = make(map[string]string)
		}
		for k, v := range opts.Headers {
			envelope.Headers[k] = v
		}
	}

	data, err := cbor.Marshal(envelope)
	if err != nil {
		// Fallback to raw body if CBOR encoding fails
		return msg.Body
	}
	return data
}

func decodePubSubPayload(payload []byte) mq.Message {
	var envelope pubSubEnvelope
	if err := cbor.Unmarshal(payload, &envelope); err != nil {
		// Fallback: treat as raw body if CBOR decoding fails
		return mq.Message{
			Body: payload,
		}
	}

	ts := time.Time{}
	if envelope.Timestamp > 0 {
		ts = time.Unix(0, envelope.Timestamp).UTC()
	}

	return mq.Message{
		Key:         envelope.Key,
		Body:        envelope.Body,
		Headers:     envelope.Headers,
		ContentType: envelope.ContentType,
		Timestamp:   ts,
	}
}
