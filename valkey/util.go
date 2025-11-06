package valkeymq

import (
	"fmt"
	"time"
)

func generateConsumerName() string {
	return fmt.Sprintf("valkey-consumer-%d", time.Now().UnixNano())
}
