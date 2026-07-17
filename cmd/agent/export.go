package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"unsafe"

	"github.com/nezhahq/agent/pkg/agent"
)

var (
	cancelFunc context.CancelFunc
)

//export StartNezhaAgent
func StartNezhaAgent(configJson *C.char) C.int {
	configStr := C.GoString(configJson)

	var cfg agent.Config
	if err := json.Unmarshal([]byte(configStr), &cfg); err != nil {
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelFunc = cancel

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	go func() {
		agent.Run(ctx, &cfg)
	}()

	return 0
}

//export StopNezhaAgent
func StopNezhaAgent() C.int {
	if cancelFunc != nil {
		cancelFunc()
	}
	return 0
}

func main() {}
