package messages

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

func TestSign(t *testing.T) {
	id := "i am test data"
	i := SHA256([]byte(id))
	count := 0
	go func() {
		select {
		case <-time.After(60 * time.Second):
			fmt.Println("count", count)
			panic("Time up")
		}
	}()
	for {
		msg, _ := NewBlockConfirmMessage(0, i)
		msg.VerifySignature()
		count++
	}
}

func SHA256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}
