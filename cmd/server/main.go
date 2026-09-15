package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/vnmchuo/gocron-dist/pkg/cluster"
	"github.com/vnmchuo/gocron-dist/pkg/hash"
	"github.com/vnmchuo/gocron-dist/pkg/scheduler"
	"github.com/vnmchuo/gocron-dist/pkg/storage"
	"github.com/vnmchuo/gocron-dist/pkg/telemetry"
	ratelimit "github.com/vnmchuo/ratelimiter"
	ratelimit_grpc "github.com/vnmchuo/ratelimiter/middleware/grpc"
	"github.com/vnmchuo/taskengine/pkg/server"
	taskenginev1 "github.com/vnmchuo/taskengine/proto/taskengine/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	nodeName := flag.String("name", "node-1", "Unique name of this cluster node")
	gossipPort := flag.Int("port", 8001, "Port for Memberlist Gossip protocol")
	grpcPort := flag.Int("grpc-port", 9001, "Port for gRPC service")
	joinAddr := flag.String("join", "", "Target node address to join cluster (e.g. localhost:8001)")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address (or 'in-memory' to auto-start embedded miniredis)")
	ingressLimit := flag.Int("ingress-limit", 60, "Max ingress requests per minute per tenant")
	egressLimit := flag.Int("egress-limit", 5, "Max egress task executions per window per target key")
	flag.Parse()

	log.Printf("=====================================================")
	log.Printf("🚀 Starting TaskEngine Node: %s", *nodeName)
	log.Printf("   gRPC Port: %d | Gossip Port: %d", *grpcPort, *gossipPort)
	log.Printf("=====================================================")

	// 1. Setup OpenTelemetry Tracer
	var tracer trace.Tracer
	tp, err := telemetry.InitTracer(*nodeName)
	if err != nil {
		log.Printf("[Telemetry] Warning: failed to init tracer: %v", err)
	} else if tp != nil {
		defer telemetry.ShutdownTracer(context.Background(), tp)
		tracer = tp.Tracer("taskengine")
	}

	// 2. Setup Redis for Distributed Rate Limiter
	var rdb *redis.Client
	if *redisAddr == "in-memory" {
		mr := miniredis.RunT(nil)
		defer mr.Close()
		log.Printf("[Redis] Using embedded in-memory Redis at %s", mr.Addr())
		rdb = redis.NewClient(&redis.Options{Addr: mr.Addr()})
	} else {
		testRdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
		ctxPing, cancelPing := context.WithTimeout(context.Background(), 1*time.Second)
		if err := testRdb.Ping(ctxPing).Err(); err != nil {
			cancelPing()
			log.Printf("[Redis] Could not connect to %s (%v). Falling back to embedded miniredis!", *redisAddr, err)
			mr, errMr := miniredis.Run()
			if errMr != nil {
				log.Fatalf("[Redis] Failed to start fallback miniredis: %v", errMr)
			}
			defer mr.Close()
			rdb = redis.NewClient(&redis.Options{Addr: mr.Addr()})
		} else {
			cancelPing()
			log.Printf("[Redis] Connected to Redis at %s", *redisAddr)
			rdb = testRdb
		}
	}

	ingressLimiter := ratelimit.NewRedisStore(
		rdb,
		ratelimit.WithLimit(*ingressLimit),
		ratelimit.WithWindow(time.Minute),
	)

	egressLimiter := ratelimit.NewRedisStore(
		rdb,
		ratelimit.WithLimit(*egressLimit),
		ratelimit.WithWindow(5*time.Second),
	)

	// 3. Initialize Consistent Hashing Ring
	ring := hash.NewConsistent()
	ring.AddNode(*nodeName)

	// 4. Initialize PebbleDB (LSM-Tree persistent storage)
	dataDir := fmt.Sprintf("data_%s", *nodeName)
	store, err := storage.NewStore(dataDir, tracer)
	if err != nil {
		log.Fatalf("[Storage] Failed to initialize PebbleDB at %s: %v", dataDir, err)
	}
	defer store.Close()
	log.Printf("[Storage] PebbleDB initialized at %s (durability enabled)", dataDir)

	// 5. Initialize Scheduler Engine
	engine := scheduler.NewEngine(tracer)
	engine.Storage = store
	engine.Ring = ring
	engine.NodeName = *nodeName

	// 6. Initialize TaskEngine Server
	srv := server.NewServer(engine, ring, nil, *nodeName, ingressLimiter, egressLimiter, tracer)

	// 7. Initialize Memberlist Cluster
	c, err := cluster.NewCluster(
		*nodeName,
		*gossipPort,
		*grpcPort,
		*joinAddr,
		func(name string) {
			log.Printf("[Cluster] Node joined: %s", name)
			ring.AddNode(name)
		},
		func(name string) {
			log.Printf("[Cluster] Node left: %s", name)
			ring.RemoveNode(name)
			go engine.Rebalance(context.Background(), srv)
		},
	)
	if err != nil {
		log.Fatalf("[Cluster] Failed to initialize Memberlist: %v", err)
	}
	srv.Cluster = c
	engine.Cluster = c

	// 8. Bootstrap gRPC Server with Ingress Rate Limiting Interceptor
	interceptor := ratelimit_grpc.RateLimiter(
		ingressLimiter,
		ratelimit_grpc.KeyFromMetadata("x-tenant-id", "default-tenant"),
	)

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	taskenginev1.RegisterTaskEngineServiceServer(grpcServer, srv)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("[gRPC] Failed to listen on port %d: %v", *grpcPort, err)
	}

	go func() {
		log.Printf("[gRPC] Service listening on :%d (Ingress RateLimit: %d req/min/tenant)", *grpcPort, *ingressLimit)
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("[gRPC] Serve error: %v", err)
		}
	}()

	// 9. Crash Recovery: reload unexecuted jobs from PebbleDB
	oldJobs, err := store.GetAllJobs()
	if err == nil && len(oldJobs) > 0 {
		log.Printf("[Recovery] Recovering %d pending jobs from PebbleDB...", len(oldJobs))
		for _, j := range oldJobs {
			engine.AddJob(j)
		}
	}

	// 10. Start Scheduler Engine Loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go engine.Run(ctx)
	log.Printf("[Engine] Priority queue scheduler loop active (Egress RateLimit: %d per 5s)", *egressLimit)

	// 11. Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Printf("\n[Shutdown] Gracefully shutting down node %s...", *nodeName)
	grpcServer.GracefulStop()
	cancel()
	time.Sleep(500 * time.Millisecond)
	log.Printf("[Shutdown] Complete. Goodbye!")
}
