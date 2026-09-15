package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vnmchuo/gocron-dist/pkg/cluster"
	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	ratelimit "github.com/vnmchuo/ratelimiter"
	taskenginev1 "github.com/vnmchuo/taskengine/proto/taskengine/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TaskRecord struct {
	State        taskenginev1.TaskState
	AssignedNode string
	RunCount     int32
	RetryCount   int32
	NextRun      time.Time
	LastError    string
	Job          *scheduler.Job
}

// Server implements taskenginev1.TaskEngineServiceServer
type Server struct {
	taskenginev1.UnimplementedTaskEngineServiceServer

	Engine         *scheduler.Engine
	Ring           *hash.Consistent
	Cluster        *cluster.Cluster
	NodeName       string
	IngressLimiter ratelimit.Limiter
	EgressLimiter  ratelimit.Limiter
	Tracer         trace.Tracer

	// Local state tracking
	tasksMu sync.RWMutex
	tasks   map[string]*TaskRecord

	// Optional custom handler callback for executed tasks
	TaskHandler func(ctx context.Context, j *scheduler.Job) error
}

func NewServer(
	engine *scheduler.Engine,
	ring *hash.Consistent,
	c *cluster.Cluster,
	nodeName string,
	ingressLimiter ratelimit.Limiter,
	egressLimiter ratelimit.Limiter,
	tracer trace.Tracer,
) *Server {
	s := &Server{
		Engine:         engine,
		Ring:           ring,
		Cluster:        c,
		NodeName:       nodeName,
		IngressLimiter: ingressLimiter,
		EgressLimiter:  egressLimiter,
		Tracer:         tracer,
		tasks:          make(map[string]*TaskRecord),
	}

	// Wire engine executor to our adaptive egress rate limiting executor
	engine.Executor = s.ExecuteTask

	// Wire Dead Letter Queue handler for exhausted retries
	engine.DLQHandler = func(ctx context.Context, j *scheduler.Job, finalErr error) {
		s.tasksMu.Lock()
		if rec, exists := s.tasks[j.ID]; exists {
			rec.State = taskenginev1.TaskState_TASK_STATE_FAILED_DLQ
			rec.RetryCount = int32(j.RetryCount)
			rec.LastError = fmt.Sprintf("DLQ: exhausted retries: %v", finalErr)
		}
		s.tasksMu.Unlock()
		log.Printf("[TaskEngine DLQ 🚨] Task %s entered Dead Letter Queue after %d attempts: %v",
			j.ID, j.RetryCount, finalErr)
	}

	return s
}

// ScheduleTask handles task scheduling with consistent hashing, inter-node routing, and egress preparation.
func (s *Server) ScheduleTask(ctx context.Context, req *taskenginev1.ScheduleTaskRequest) (*taskenginev1.ScheduleTaskResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "task ID must not be empty")
	}

	var span trace.Span
	if s.Tracer != nil {
		ctx, span = s.Tracer.Start(ctx, "ScheduleTask", trace.WithAttributes(
			attribute.String("task_id", req.Id),
			attribute.String("tenant_id", req.TenantId),
			attribute.String("rate_limit_key", req.RateLimitKey),
		))
		defer span.End()
	}

	// 1. Determine owner node using Consistent Hashing Ring
	owner := s.Ring.GetNodeWithContext(ctx, req.Id)

	// 2. If the owner is not this node, forward the request via gRPC to owner
	if owner != s.NodeName && owner != "" {
		addr, err := s.Cluster.GetNodeGrpcAddress(owner)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to resolve gRPC address for owner %q: %v", owner, err)
		}

		log.Printf("[Forward] Task %s forwarded to owner node %q (%s)", req.Id, owner, addr)

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to connect to owner node %q: %v", owner, err)
		}
		defer conn.Close()

		client := taskenginev1.NewTaskEngineServiceClient(conn)
		fwdCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		return client.ScheduleTask(fwdCtx, req)
	}

	// 3. Current node is the owner: parse execution time
	var execTime time.Time
	if req.ScheduleTime != nil && req.ScheduleTime.AsTime().After(time.Now()) {
		execTime = req.ScheduleTime.AsTime()
	} else {
		execTime = time.Now()
	}

	weight := int(req.Weight)
	if weight <= 0 {
		weight = 1
	}

	job := &scheduler.Job{
		ID:             req.Id,
		Payload:        req.Payload,
		NextRun:        execTime,
		RepeatInterval: time.Duration(req.RepeatIntervalNanos),
		MaxRuns:        int(req.MaxRuns),
		RateLimitKey:   req.RateLimitKey,
		Weight:         weight,
		MaxRetries:     int(req.MaxRetries),
		InitialBackoff: time.Duration(req.InitialBackoffMs) * time.Millisecond,
		MaxBackoff:     time.Duration(req.MaxBackoffMs) * time.Millisecond,
	}

	// 4. Save record locally
	s.tasksMu.Lock()
	s.tasks[req.Id] = &TaskRecord{
		State:        taskenginev1.TaskState_TASK_STATE_PENDING,
		AssignedNode: s.NodeName,
		NextRun:      execTime,
		Job:          job,
	}
	s.tasksMu.Unlock()

	// 5. Add to local engine (persists to PebbleDB WAL and pushes to Min-Heap)
	s.Engine.AddJobWithContext(ctx, job)

	// 6. Inspect remaining ingress quota for client feedback
	var remainingQuota int64
	if s.IngressLimiter != nil && req.TenantId != "" {
		if stat, err := s.IngressLimiter.Status(ctx, req.TenantId); err == nil {
			remainingQuota = stat.Remaining
		}
	}

	return &taskenginev1.ScheduleTaskResponse{
		Success:               true,
		Message:               "Task scheduled successfully",
		AssignedNode:          s.NodeName,
		IngressRemainingQuota: remainingQuota,
	}, nil
}

// ExecuteTask is the adaptive egress rate limiting executor plugged into gocron-dist scheduler Engine.
func (s *Server) ExecuteTask(ctx context.Context, j *scheduler.Job) error {
	// 1. Egress Throttling Check
	if j.RateLimitKey != "" && s.EgressLimiter != nil {
		weight := j.Weight
		if weight <= 0 {
			weight = 1
		}

		res, err := s.EgressLimiter.AllowN(ctx, j.RateLimitKey, weight)
		if err != nil {
			log.Printf("[Egress Error] Rate limiter check failed for key %q: %v", j.RateLimitKey, err)
			return fmt.Errorf("egress rate limiter error: %w", err)
		}

		if !res.Allowed {
			// Throttled! Do NOT discard job.
			backoff := res.ResetAfter
			if backoff <= 0 {
				backoff = 500 * time.Millisecond
			}

			j.NextRun = time.Now().Add(backoff)

			s.tasksMu.Lock()
			if rec, exists := s.tasks[j.ID]; exists {
				rec.State = taskenginev1.TaskState_TASK_STATE_THROTTLED
				rec.NextRun = j.NextRun
				rec.LastError = fmt.Sprintf("throttled by %s, reset in %s", j.RateLimitKey, backoff)
			}
			s.tasksMu.Unlock()

			log.Printf("[Egress Throttled ⏳] Task %s (target: %q, weight: %d) hit rate limit. Re-queuing for %s (backoff: %v)",
				j.ID, j.RateLimitKey, weight, j.NextRun.Format("15:04:05.000"), backoff)

			// Reschedule task back into priority queue with updated NextRun
			s.Engine.AddJobWithContext(ctx, j)
			return scheduler.ErrJobDeferred
		}
	}

	// 2. Allowed! Transition to EXECUTING
	s.tasksMu.Lock()
	if rec, exists := s.tasks[j.ID]; exists {
		rec.State = taskenginev1.TaskState_TASK_STATE_EXECUTING
		rec.RunCount++
	}
	s.tasksMu.Unlock()

	log.Printf("[Egress Executed 🚀] Task %s executed! Key: %q, Payload: %s", j.ID, j.RateLimitKey, j.Payload)

	// 3. Invoke custom handler if provided
	if s.TaskHandler != nil {
		if err := s.TaskHandler(ctx, j); err != nil {
			log.Printf("[TaskHandler Error] Task %s failed: %v", j.ID, err)
			s.tasksMu.Lock()
			if rec, exists := s.tasks[j.ID]; exists {
				rec.LastError = err.Error()
			}
			s.tasksMu.Unlock()
			return err
		}
	}

	// 4. Mark COMPLETED
	s.tasksMu.Lock()
	if rec, exists := s.tasks[j.ID]; exists {
		rec.State = taskenginev1.TaskState_TASK_STATE_COMPLETED
	}
	s.tasksMu.Unlock()

	return nil
}

// InspectQuota inspects the remaining quota for any key (ingress or egress) without consuming tokens.
func (s *Server) InspectQuota(ctx context.Context, req *taskenginev1.InspectQuotaRequest) (*taskenginev1.InspectQuotaResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	// First try egress limiter, fallback to ingress
	limiter := s.EgressLimiter
	if limiter == nil {
		limiter = s.IngressLimiter
	}
	if limiter == nil {
		return nil, status.Error(codes.FailedPrecondition, "rate limiter not configured on this node")
	}

	res, err := limiter.Status(ctx, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to inspect quota: %v", err)
	}

	return &taskenginev1.InspectQuotaResponse{
		Key:          req.Key,
		Remaining:    res.Remaining,
		Limit:        int32(res.Limit),
		ResetAfterMs: res.ResetAfter.Milliseconds(),
	}, nil
}

// GetTaskStatus retrieves task metadata and current lifecycle state.
func (s *Server) GetTaskStatus(ctx context.Context, req *taskenginev1.GetTaskStatusRequest) (*taskenginev1.GetTaskStatusResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "task ID must not be empty")
	}

	owner := s.Ring.GetNodeWithContext(ctx, req.Id)
	if owner != s.NodeName && owner != "" {
		addr, err := s.Cluster.GetNodeGrpcAddress(owner)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to resolve owner %q: %v", owner, err)
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to connect to owner %q: %v", owner, err)
		}
		defer conn.Close()

		client := taskenginev1.NewTaskEngineServiceClient(conn)
		return client.GetTaskStatus(ctx, req)
	}

	s.tasksMu.RLock()
	rec, exists := s.tasks[req.Id]
	s.tasksMu.RUnlock()

	if !exists {
		return nil, status.Errorf(codes.NotFound, "task %q not found on node %q", req.Id, s.NodeName)
	}

	return &taskenginev1.GetTaskStatusResponse{
		Id:           req.Id,
		State:        rec.State,
		AssignedNode: rec.AssignedNode,
		RunCount:     rec.RunCount,
		NextRun:      timestamppb.New(rec.NextRun),
		LastError:    rec.LastError,
		RetryCount:   rec.RetryCount,
	}, nil
}

// CancelTask cancels and deletes a task from storage and queue.
func (s *Server) CancelTask(ctx context.Context, req *taskenginev1.CancelTaskRequest) (*taskenginev1.CancelTaskResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "task ID must not be empty")
	}

	owner := s.Ring.GetNodeWithContext(ctx, req.Id)
	if owner != s.NodeName && owner != "" {
		addr, err := s.Cluster.GetNodeGrpcAddress(owner)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to resolve owner %q: %v", owner, err)
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to connect to owner %q: %v", owner, err)
		}
		defer conn.Close()

		client := taskenginev1.NewTaskEngineServiceClient(conn)
		return client.CancelTask(ctx, req)
	}

	s.tasksMu.Lock()
	delete(s.tasks, req.Id)
	s.tasksMu.Unlock()

	if s.Engine.Storage != nil {
		_ = s.Engine.Storage.DeleteJobWithContext(ctx, req.Id)
	}

	return &taskenginev1.CancelTaskResponse{
		Success: true,
		Message: fmt.Sprintf("Task %q canceled on node %q", req.Id, s.NodeName),
	}, nil
}

// ForwardJob satisfies scheduler.Forwarder interface for cluster rebalancing.
func (s *Server) ForwardJob(ctx context.Context, j *scheduler.Job) error {
	req := &taskenginev1.ScheduleTaskRequest{
		Id:                  j.ID,
		Payload:             j.Payload,
		ScheduleTime:        timestamppb.New(j.NextRun),
		RepeatIntervalNanos: int64(j.RepeatInterval),
		MaxRuns:             int32(j.MaxRuns),
		RateLimitKey:        j.RateLimitKey,
		Weight:              int32(j.Weight),
		MaxRetries:          int32(j.MaxRetries),
		InitialBackoffMs:    j.InitialBackoff.Milliseconds(),
		MaxBackoffMs:        j.MaxBackoff.Milliseconds(),
	}
	_, err := s.ScheduleTask(ctx, req)
	return err
}

