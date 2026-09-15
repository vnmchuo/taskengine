package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	taskenginev1 "github.com/vnmchuo/taskengine/proto/taskengine/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "schedule":
		handleSchedule(os.Args[2:])
	case "burst-ingress":
		handleBurstIngress(os.Args[2:])
	case "burst-egress":
		handleBurstEgress(os.Args[2:])
	case "quota":
		handleQuota(os.Args[2:])
	case "status":
		handleStatus(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`TaskEngine CLI Client

Commands:
  schedule        Schedule a single task
  burst-ingress   Send rapid burst requests to demonstrate Ingress Rate Limiting
  burst-egress    Schedule multiple tasks targeting the same external key to test Egress Throttling
  quota           Inspect rate-limit quota for a key without consuming it
  status          Check task execution status

Examples:
  go run cmd/client/main.go schedule -target localhost:9001 -id task-1 -tenant acme -key vendor:openai -weight 1 -delay 2 -msg "Summarize text"
  go run cmd/client/main.go burst-ingress -target localhost:9001 -tenant spammer -count 15
  go run cmd/client/main.go burst-egress -target localhost:9001 -key vendor:sms -count 8
  go run cmd/client/main.go quota -target localhost:9001 -key vendor:sms
  go run cmd/client/main.go status -target localhost:9001 -id task-1`)
}

func dial(target string) (*grpc.ClientConn, taskenginev1.TaskEngineServiceClient) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", target, err)
	}
	return conn, taskenginev1.NewTaskEngineServiceClient(conn)
}

func handleSchedule(args []string) {
	fs := flag.NewFlagSet("schedule", flag.ExitOnError)
	target := fs.String("target", "localhost:9001", "gRPC server target")
	id := fs.String("id", fmt.Sprintf("task-%d", time.Now().UnixNano()%100000), "Task ID")
	tenant := fs.String("tenant", "default-tenant", "Tenant ID for ingress rate limiting")
	rateLimitKey := fs.String("key", "vendor:default", "Egress rate limit key")
	weight := fs.Int("weight", 1, "Egress token weight")
	delay := fs.Int("delay", 0, "Delay in seconds before execution")
	msg := fs.String("msg", "Hello TaskEngine", "Payload message")
	repeatSec := fs.Int("repeat", 0, "Repeat interval in seconds (0 = one-off)")
	maxRuns := fs.Int("max-runs", 0, "Max execution count (0 = unlimited)")
	retries := fs.Int("retries", 0, "Max retry attempts on failure (0 = no retry)")
	fs.Parse(args)

	conn, client := dial(*target)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "x-tenant-id", *tenant)

	execTime := time.Now().Add(time.Duration(*delay) * time.Second)

	req := &taskenginev1.ScheduleTaskRequest{
		Id:                  *id,
		TenantId:            *tenant,
		RateLimitKey:        *rateLimitKey,
		Weight:              int32(*weight),
		ScheduleTime:        timestamppb.New(execTime),
		Payload:             *msg,
		RepeatIntervalNanos: int64(time.Duration(*repeatSec) * time.Second),
		MaxRuns:             int32(*maxRuns),
		MaxRetries:          int32(*retries),
		InitialBackoffMs:    1000,
		MaxBackoffMs:        30000,
	}

	resp, err := client.ScheduleTask(ctx, req)
	if err != nil {
		st, _ := status.FromError(err)
		log.Fatalf("❌ Schedule failed: [%s] %s", st.Code(), st.Message())
	}

	fmt.Printf("✅ Task Scheduled Successfully!\n")
	fmt.Printf("   Task ID:         %s\n", *id)
	fmt.Printf("   Assigned Node:   %s\n", resp.AssignedNode)
	fmt.Printf("   Execute At:      %s\n", execTime.Format("15:04:05"))
	fmt.Printf("   Egress Target:   %s (weight: %d)\n", *rateLimitKey, *weight)
	fmt.Printf("   Max Retries:     %d\n", *retries)
	fmt.Printf("   Ingress Quota:   %d remaining\n", resp.IngressRemainingQuota)
}

func handleBurstIngress(args []string) {
	fs := flag.NewFlagSet("burst-ingress", flag.ExitOnError)
	target := fs.String("target", "localhost:9001", "gRPC server target")
	tenant := fs.String("tenant", "spammer-tenant", "Tenant ID to burst")
	count := fs.Int("count", 20, "Number of rapid requests to send")
	fs.Parse(args)

	conn, client := dial(*target)
	defer conn.Close()

	fmt.Printf("⚡ Firing %d rapid requests as tenant %q to test Ingress Rate Limiting...\n\n", *count, *tenant)

	allowed := 0
	blocked := 0

	for i := 1; i <= *count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-tenant-id", *tenant)

		taskID := fmt.Sprintf("burst-ing-%d", i)
		_, err := client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
			Id:           taskID,
			TenantId:     *tenant,
			Payload:      "burst-ingress-data",
			ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
		})
		cancel()

		if err != nil {
			st, _ := status.FromError(err)
			fmt.Printf("  Req #%02d: 🔴 BLOCKED [%s]: %s\n", i, st.Code(), st.Message())
			blocked++
		} else {
			fmt.Printf("  Req #%02d: 🟢 ALLOWED (Accepted by cluster)\n", i)
			allowed++
		}
	}

	fmt.Printf("\n📊 Ingress Burst Summary: %d Allowed, %d Blocked (HTTP/gRPC 429 ResourceExhausted)\n", allowed, blocked)
}

func handleBurstEgress(args []string) {
	fs := flag.NewFlagSet("burst-egress", flag.ExitOnError)
	target := fs.String("target", "localhost:9001", "gRPC server target")
	key := fs.String("key", "vendor:sms", "Egress rate limit target key")
	count := fs.Int("count", 8, "Number of tasks to schedule")
	fs.Parse(args)

	conn, client := dial(*target)
	defer conn.Close()

	fmt.Printf("⚡ Scheduling %d tasks due immediately targeting egress key %q...\n", *count, *key)
	fmt.Println("   Observe the server logs: Tasks within quota execute instantly;")
	fmt.Println("   Tasks exceeding the rate limit are throttled and deferred with backoff!")
	fmt.Println()

	for i := 1; i <= *count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-tenant-id", fmt.Sprintf("tenant-%d", i))

		taskID := fmt.Sprintf("egress-task-%d", i)
		resp, err := client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
			Id:           taskID,
			TenantId:     fmt.Sprintf("tenant-%d", i),
			RateLimitKey: *key,
			Weight:       1,
			Payload:      fmt.Sprintf("Send SMS notification #%d", i),
			ScheduleTime: timestamppb.New(time.Now()),
		})
		cancel()

		if err != nil {
			fmt.Printf("  Failed to schedule %s: %v\n", taskID, err)
		} else {
			fmt.Printf("  Scheduled %s on %s\n", taskID, resp.AssignedNode)
		}
	}

	fmt.Println("\n✅ All tasks submitted to scheduler. Check server logs to see the adaptive pacing in real time!")
}

func handleQuota(args []string) {
	fs := flag.NewFlagSet("quota", flag.ExitOnError)
	target := fs.String("target", "localhost:9001", "gRPC server target")
	key := fs.String("key", "vendor:sms", "Rate limit key to inspect")
	fs.Parse(args)

	conn, client := dial(*target)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.InspectQuota(ctx, &taskenginev1.InspectQuotaRequest{Key: *key})
	if err != nil {
		log.Fatalf("Failed to inspect quota: %v", err)
	}

	fmt.Printf("🔍 Rate Limit Quota for key %q:\n", resp.Key)
	fmt.Printf("   Remaining:       %d units\n", resp.Remaining)
	fmt.Printf("   Limit:           %d units\n", resp.Limit)
	fmt.Printf("   Window Reset In: %d ms\n", resp.ResetAfterMs)
}

func handleStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	target := fs.String("target", "localhost:9001", "gRPC server target")
	id := fs.String("id", "", "Task ID to inspect")
	fs.Parse(args)

	if *id == "" {
		log.Fatal("Please specify -id <task-id>")
	}

	conn, client := dial(*target)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.GetTaskStatus(ctx, &taskenginev1.GetTaskStatusRequest{Id: *id})
	if err != nil {
		log.Fatalf("Failed to get task status: %v", err)
	}

	fmt.Printf("📋 Task Status for %q:\n", resp.Id)
	fmt.Printf("   State:         %s\n", resp.State.String())
	fmt.Printf("   Assigned Node: %s\n", resp.AssignedNode)
	fmt.Printf("   Run Count:     %d\n", resp.RunCount)
	if resp.NextRun != nil {
		fmt.Printf("   Next Run:      %s\n", resp.NextRun.AsTime().Format("15:04:05"))
	}
	if resp.LastError != "" {
		fmt.Printf("   Last Notice:   %s\n", resp.LastError)
	}
}
