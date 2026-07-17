package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"crypto/tls"
	"encoding/json"
	"sync"
	"time"

	"github.com/blang/semver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nezhahq/agent/model"
	"github.com/nezhahq/agent/pkg/monitor"
	pb "github.com/nezhahq/agent/proto"
)

var (
	agentCtx    context.Context
	agentCancel context.CancelFunc
	agentWG     sync.WaitGroup
	agentConn   *grpc.ClientConn // 用于优雅关闭连接
)

//export StartNezhaAgent
func StartNezhaAgent(configJson *C.char) C.int {
	configStr := C.GoString(configJson)

	var wrapper struct {
		Config string `json:"config"`
	}
	if err := json.Unmarshal([]byte(configStr), &wrapper); err != nil {
		return 1
	}

	if err := preRun(wrapper.Config); err != nil {
		return 1
	}

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
	if agentConn != nil {
		agentConn.Close()
	}
	agentWG.Wait()
	return 0
}

func runWithContext(ctx context.Context) {
	publishCredentials(agentConfig)

	// 检查更新（默认禁用自动更新，保留逻辑）
	if _, err := semver.Parse(version); err == nil && !agentConfig.DisableAutoUpdate {
		if doSelfUpdate(true) {
			return
		}
		go func() {
			// 定期更新 goroutine 省略（默认禁用）
		}()
	}

	var dashboardBootTimeReceipt *pb.Uint64Receipt

	retry := func() {
		initialized = false
		if agentConn != nil {
			agentConn.Close()
		}
		select {
		case <-time.After(delayWhenError):
		case <-ctx.Done():
			return
		}
		println("Try to reconnect ...")
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 每次重连重建认证信息
		auth := model.AuthHandler{
			Credentials: func() (string, string) {
				c := loadCredentials()
				return c.ClientSecret, c.ClientUUID
			},
			RequireTLS: func() bool {
				return agentConfig.TLS
			},
		}

		var securityOption grpc.DialOption
		if agentConfig.TLS {
			if agentConfig.InsecureTLS {
				securityOption = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
					MinVersion:         tls.VersionTLS12,
					InsecureSkipVerify: true,
				}))
			} else {
				securityOption = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
					MinVersion: tls.VersionTLS12,
				}))
			}
		} else {
			securityOption = grpc.WithTransportCredentials(insecure.NewCredentials())
		}

		conn, err := grpc.NewClient(agentConfig.Server, securityOption, grpc.WithPerRPCCredentials(&auth))
		if err != nil {
			printf("与面板建立连接失败: %v", err)
			retry()
			if ctx.Err() != nil {
				return
			}
			continue
		}
		agentConn = conn
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

		if ctx.Err() != nil {
			return
		}
	}
}
