package server_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	ratelimit "github.com/vnmchuo/ratelimiter"
	ratelimit_grpc "github.com/vnmchuo/ratelimiter/middleware/grpc"
	"github.com/vnmchuo/taskengine/pkg/server"
	taskenginev1 "github.com/vnmchuo/taskengine/proto/taskengine/v1"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const bufSize = 1024 * 1024

var noopTracer = noop.NewTracerProvider().Tracer("test")

type mockStore struct {
	jobs map[string]*scheduler.Job
}

func newMockStore() *mockStore {
	return &mockStore{jobs: make(map[string]*scheduler.Job)}
}

func (m *mockStore) SaveJob(j *scheduler.Job) error {
	m.jobs[j.ID] = j
	return nil
}
func (m *mockStore) SaveJobWithContext(ctx context.Context, j *scheduler.Job) error {
	return m.SaveJob(j)
}
func (m *mockStore) GetJob(id string) (*scheduler.Job, error) {
	return m.jobs[id], nil
}
func (m *mockStore) GetAllJobs() ([]*scheduler.Job, error) {
	var list []*scheduler.Job
	for _, j := range m.jobs {
		list = append(list, j)
	}
	return list, nil
}
func (m *mockStore) DeleteJob(id string) error {
	delete(m.jobs, id)
	return nil
}
func (m *mockStore) DeleteJobWithContext(ctx context.Context, id string) error {
	return m.DeleteJob(id)
}
func (m *mockStore) Close() error { return nil }

func setupTestEnvironment(t *testing.T, ingressLimit, egressLimit int, egressWindow time.Duration) (
	*miniredis.Miniredis,
	taskenginev1.TaskEngineServiceClient,
	*server.Server,
	func(),
) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	ingressLimiter := ratelimit.NewRedisStore(
		rdb,
		ratelimit.WithLimit(ingressLimit),
		ratelimit.WithWindow(time.Minute),
	)

	egressLimiter := ratelimit.NewRedisStore(
		rdb,
		ratelimit.WithLimit(egressLimit),
		ratelimit.WithWindow(egressWindow),
	)

	engine := scheduler.NewEngine(noopTracer)
	engine.Storage = newMockStore()

	ring := hash.NewConsistent()
	ring.AddNode("node-test")

	srv := server.NewServer(engine, ring, nil, "node-test", ingressLimiter, egressLimiter, noopTracer)

	// Ingress rate limit interceptor
	interceptor := ratelimit_grpc.RateLimiter(ingressLimiter, ratelimit_grpc.KeyFromMetadata("x-tenant-id", "default-tenant"))

	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	taskenginev1.RegisterTaskEngineServiceServer(grpcServer, srv)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("grpc server stopped: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := taskenginev1.NewTaskEngineServiceClient(conn)

	cleanup := func() {
		conn.Close()
		grpcServer.GracefulStop()
		mr.Close()
	}

	return mr, client, srv, cleanup
}

func TestTaskEngine_IngressRateLimiting(t *testing.T) {
	_, client, _, cleanup := setupTestEnvironment(t, 2, 100, time.Minute)
	defer cleanup()

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-tenant-id", "tenant-alpha"))

	// 1st request -> allowed
	resp, err := client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
		Id:           "task-1",
		TenantId:     "tenant-alpha",
		Payload:      "payload-1",
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if err != nil || !resp.Success {
		t.Fatalf("expected task 1 to succeed, got %v", err)
	}

	// 2nd request -> allowed
	resp, err = client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
		Id:           "task-2",
		TenantId:     "tenant-alpha",
		Payload:      "payload-2",
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if err != nil || !resp.Success {
		t.Fatalf("expected task 2 to succeed, got %v", err)
	}

	// 3rd request -> blocked by Ingress Rate Limiter (ResourceExhausted)
	_, err = client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
		Id:           "task-3",
		TenantId:     "tenant-alpha",
		Payload:      "payload-3",
		ScheduleTime: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if err == nil {
		t.Fatal("expected task 3 to be blocked by ingress rate limiter, but succeeded")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected codes.ResourceExhausted, got %v", err)
	}
}

func TestTaskEngine_EgressAdaptiveThrottling(t *testing.T) {
	// Egress limit: 2 requests per 500ms
	_, client, srv, cleanup := setupTestEnvironment(t, 100, 2, 500*time.Millisecond)
	defer cleanup()

	var executedCount atomic.Int32
	srv.TaskHandler = func(ctx context.Context, j *scheduler.Job) error {
		executedCount.Add(1)
		return nil
	}

	// Start scheduler engine loop in background
	ctxEngine, cancelEngine := context.WithCancel(context.Background())
	defer cancelEngine()
	go srv.Engine.Run(ctxEngine)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-tenant-id", "tenant-tester"))

	// Schedule 4 tasks due immediately with RateLimitKey="vendor:webhook"
	for i := 1; i <= 4; i++ {
		_, err := client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
			Id:           string(rune('A' + i)),
			TenantId:     "tenant-tester",
			RateLimitKey: "vendor:webhook",
			Weight:       1,
			Payload:      "burst-task",
			ScheduleTime: timestamppb.New(time.Now().Add(10 * time.Millisecond)),
		})
		if err != nil {
			t.Fatalf("failed to schedule task: %v", err)
		}
	}

	// Wait 100ms: first 2 tasks should execute, 3rd and 4th should be throttled & deferred
	time.Sleep(200 * time.Millisecond)

	count := executedCount.Load()
	if count != 2 {
		t.Fatalf("expected exactly 2 tasks executed within quota window, got %d", count)
	}

	// Wait for window reset (> 500ms), remaining deferred tasks should now be dispatched!
	time.Sleep(600 * time.Millisecond)

	count = executedCount.Load()
	if count != 4 {
		t.Fatalf("expected all 4 tasks eventually executed after window roll, got %d", count)
	}
}

func TestTaskEngine_InspectQuota(t *testing.T) {
	_, client, _, cleanup := setupTestEnvironment(t, 10, 5, time.Minute)
	defer cleanup()

	resp, err := client.InspectQuota(context.Background(), &taskenginev1.InspectQuotaRequest{
		Key: "vendor:test",
	})
	if err != nil {
		t.Fatalf("failed to inspect quota: %v", err)
	}

	if resp.Limit != 5 || resp.Remaining != 5 {
		t.Fatalf("expected limit 5 remaining 5, got limit=%d remaining=%d", resp.Limit, resp.Remaining)
	}
}

func TestTaskEngine_RetryAndDLQ(t *testing.T) {
	_, client, srv, cleanup := setupTestEnvironment(t, 100, 100, time.Minute)
	defer cleanup()

	var attempts atomic.Int32
	srv.TaskHandler = func(ctx context.Context, j *scheduler.Job) error {
		attempts.Add(1)
		return errors.New("downstream webhook timeout")
	}

	ctxEngine, cancelEngine := context.WithCancel(context.Background())
	defer cancelEngine()
	go srv.Engine.Run(ctxEngine)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-tenant-id", "tenant-retry"))

	// Schedule with MaxRetries=2, InitialBackoffMs=20, MaxBackoffMs=20
	_, err := client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
		Id:               "task-fail-dlq",
		TenantId:         "tenant-retry",
		Payload:          "important-event",
		ScheduleTime:     timestamppb.New(time.Now().Add(10 * time.Millisecond)),
		MaxRetries:       2,
		InitialBackoffMs: 20,
		MaxBackoffMs:     20,
	})
	if err != nil {
		t.Fatalf("failed to schedule task: %v", err)
	}

	// Wait for retries to exhaust (1 initial + 2 retries = 3 attempts)
	time.Sleep(300 * time.Millisecond)

	statusResp, err := client.GetTaskStatus(ctx, &taskenginev1.GetTaskStatusRequest{Id: "task-fail-dlq"})
	if err != nil {
		t.Fatalf("failed to get task status: %v", err)
	}

	if statusResp.State != taskenginev1.TaskState_TASK_STATE_FAILED_DLQ {
		t.Fatalf("expected state TASK_STATE_FAILED_DLQ, got %s", statusResp.State)
	}
	if statusResp.RetryCount != 2 {
		t.Fatalf("expected retry count 2, got %d", statusResp.RetryCount)
	}
}

func TestTaskEngine_RetryEventualSuccess(t *testing.T) {
	_, client, srv, cleanup := setupTestEnvironment(t, 100, 100, time.Minute)
	defer cleanup()

	var attempts atomic.Int32
	srv.TaskHandler = func(ctx context.Context, j *scheduler.Job) error {
		attempt := attempts.Add(1)
		if attempt < 2 {
			return errors.New("temporary blip")
		}
		return nil // Success on attempt 2
	}

	ctxEngine, cancelEngine := context.WithCancel(context.Background())
	defer cancelEngine()
	go srv.Engine.Run(ctxEngine)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-tenant-id", "tenant-retry"))

	_, err := client.ScheduleTask(ctx, &taskenginev1.ScheduleTaskRequest{
		Id:               "task-retry-success",
		TenantId:         "tenant-retry",
		Payload:          "resilient-work",
		ScheduleTime:     timestamppb.New(time.Now().Add(10 * time.Millisecond)),
		MaxRetries:       3,
		InitialBackoffMs: 20,
		MaxBackoffMs:     20,
	})
	if err != nil {
		t.Fatalf("failed to schedule task: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	statusResp, err := client.GetTaskStatus(ctx, &taskenginev1.GetTaskStatusRequest{Id: "task-retry-success"})
	if err != nil {
		t.Fatalf("failed to get task status: %v", err)
	}

	if statusResp.State != taskenginev1.TaskState_TASK_STATE_COMPLETED {
		t.Fatalf("expected state TASK_STATE_COMPLETED, got %s", statusResp.State)
	}
}
