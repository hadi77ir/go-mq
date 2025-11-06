package valkeymq

import (
	"context"
	"strconv"

	"github.com/hadi77ir/go-mq"
	valkey "github.com/valkey-io/valkey-go"
)

func appendStreamEntry(ctx context.Context, cfg Config, client valkey.Client, stream string, msg mq.Message, opts publishOptsInternal) error {
	fieldValues := encodeMessage(msg, opts)

	var builder valkey.Completed
	maxLen := strconv.FormatInt(cfg.StreamMaxLen, 10)

	switch {
	case cfg.StreamMaxLen > 0 && cfg.ApproximateTrimming:
		fv := client.B().Xadd().Key(stream).Maxlen().Almost().Threshold(maxLen).Id("*").FieldValue()
		for field, value := range fieldValues {
			fv = fv.FieldValue(field, value)
		}
		builder = fv.Build()
	case cfg.StreamMaxLen > 0:
		fv := client.B().Xadd().Key(stream).Maxlen().Exact().Threshold(maxLen).Id("*").FieldValue()
		for field, value := range fieldValues {
			fv = fv.FieldValue(field, value)
		}
		builder = fv.Build()
	default:
		fv := client.B().Xadd().Key(stream).Id("*").FieldValue()
		for field, value := range fieldValues {
			fv = fv.FieldValue(field, value)
		}
		builder = fv.Build()
	}

	return client.Do(ctx, builder).Error()
}
