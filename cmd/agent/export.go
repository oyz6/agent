package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

var (
	agentCtx    context.Context
	agentCancel context.CancelFunc
	agentWG     sync.WaitGroup
)

//export StartNezhaAgent
func StartNezhaAgent(configJson *C.char) C.int {
	configStr := C.GoString(configJson)

	// 解析传入的 JSON: {"config":"/path/to/config.yaml"}
	var wrapper struct {
		Config string `json:"config"`
	}
	if err := json.Unmarshal([]byte(configStr), &wrapper); err != nil {
		return 1
	}

	// 初始化配置（与 main 中 preRun 相同）
	if err := preRun(wrapper.Config); err != nil {
		return 1
	}

	// 创建可取消的上下文
	agentCtx, agentCancel = context.WithCancel(context.Background())

	agentWG.Add(1)
	go func() {
		defer agentWG.Done()
		runWithContext(agentCtx)
	}()

	return 0
}

//export StopNezhaAgent
func StopNezhaAgent() C.int {
	if agentCancel != nil {
		agentCancel()
	}
	// 关闭现有连接，使阻塞的 RPC 快速退出
	if conn != nil {
		conn.Close()
	}
	// 等待 agent 主循环结束
	agentWG.Wait()
	return 0
}

// runWithContext 是 run() 的拷贝，添加了 context 控制
func runWithContext(ctx context.Context) {
	// 初始化凭据快照（原 run 的第一步）
	publishCredentials(agentConfig)

	// 检查更新（若不禁用，可能会退出进程，但配置已禁用）
	// 此处保留原逻辑，但受到 DisableAutoUpdate 控制，故安全
	if _, err := semver.Parse(version); err == nil && !agentConfig.DisableAutoUpdate {
		if doSelfUpdate(true) {
			return
		}
		go func() {
			// 省略定期更新检查，因为默认已禁用
		}()
	}

	var err error
	var dashboardBootTimeReceipt *pb.Uint64Receipt
	// 注意：conn 是全局变量，直接使用
	// var conn *grpc.ClientConn 已声明在 main.go 中

	retry := func() {
		initialized = false
		if conn != nil {
			conn.Close()
		}
		// 改为可中断的等待
		select {
		case <-time.After(delayWhenError):
		case <-ctx.Done():
			return
		}
		println("Try to reconnect ...")
	}

	for {
		// 检查是否应该退出
		select {
		case <-ctx.Done():
			return
		default:
		}

		var securityOption grpc.DialOption
		if agentConfig.TLS {
			if agentConfig.InsecureTLS {
				securityOption = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}))
			} else {
				securityOption = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}))
			}
		} else {
			securityOption = grpc.WithTransportCredentials(insecure.NewCredentials())
		}

		conn, err = grpc.NewClient(agentConfig.Server, securityOption, grpc.WithPerRPCCredentials(&auth))
		if err != nil {
			printf("与面板建立连接失败: %v", err)
			retry()
			// retry 内部可能因 ctx.Done() 返回，需要再次检查
			if ctx.Err() != nil {
				return
			}
			continue
		}
		client = pb.NewNezhaServiceClient(conn)
		printf("Connection to %s established", agentConfig.Server)

		timeOutCtx, cancel := context.WithTimeout(ctx, networkTimeOut)
		dashboardBootTimeReceipt, err = client.ReportSystemInfo2(timeOutCtx, monitor.GetHost().PB())
		cancel()
		if err != nil {
			printf("上报系统信息失败: %v", err)
			retry()
			if ctx.Err() != nil {
				return
			}
			continue
		}

		geoipReported = geoipReported && prevDashboardBootTime > 0 && dashboardBootTimeReceipt.GetData() == prevDashboardBootTime
		prevDashboardBootTime = dashboardBootTimeReceipt.GetData()
		initialized = true

		wCtx, wCancel := context.WithCancel(ctx)

		// 请求任务流
		tasks, err := doWithTimeout(func() (pb.NezhaService_RequestTaskClient, error) {
			return client.RequestTask(wCtx)
		}, networkTimeOut)
		if err != nil {
			printf("请求任务失败: %v", err)
			wCancel()
			retry()
			if ctx.Err() != nil {
				return
			}
			continue
		}
		go receiveTasksDaemon(tasks, wCancel)

		reportState, err := doWithTimeout(func() (pb.NezhaService_ReportSystemStateClient, error) {
			return client.ReportSystemState(wCtx)
		}, networkTimeOut)
		if err != nil {
			printf("上报状态信息失败: %v", err)
			wCancel()
			retry()
			if ctx.Err() != nil {
				return
			}
			continue
		}
		go reportStateDaemon(reportState, wCancel)

		// 等待退出信号：context 取消 或 重载信号
		select {
		case <-reloadSigChan:
			println("Reloading...")
			wCancel()
		case <-wCtx.Done():
			println("Worker exit...")
		case <-ctx.Done():
			wCancel()
			return
		}

		// 重试前再次检查 ctx
		if ctx.Err() != nil {
			return
		}
	}
}

func main() {} // 必须存在，但不会执行
