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

	"github.com/nezhahq/agent/agent"   // 修正导入路径
)

var cancelFunc context.CancelFunc

//export StartNezhaAgent
func StartNezhaAgent(configJson *C.char) C.int {
	configStr := C.GoString(configJson)

	// 解析配置，假设原版脚本传入的是 {"config":"/path/to/config.yaml"}
	var wrapper struct {
		Config string `json:"config"`
	}
	if err := json.Unmarshal([]byte(configStr), &wrapper); err != nil {
		return 1
	}

	// 从 YAML 文件加载 agent.Config（需要 gopkg.in/yaml.v3 已存在）
	cfg, err := agent.LoadConfig(wrapper.Config)
	if err != nil {
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
		agent.Run(ctx, cfg)
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
