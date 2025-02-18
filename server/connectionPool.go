package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connItem wraps a gRPC connection with a lastUsed timestamp.
type connItem struct {
	conn     *grpc.ClientConn
	lastUsed time.Time
}

// ConnectionPool caches gRPC connections keyed by target address.
type ConnectionPool struct {
	mu           sync.Mutex
	connections  map[string]*connItem
	idleTimeout  time.Duration
	cleanupDelay time.Duration
}

// NewConnectionPool initializes a new connection pool with the given idle timeout.
// cleanupDelay determines how often the janitor will scan for idle connections.
func NewConnectionPool(idleTimeout, cleanupDelay time.Duration) *ConnectionPool {
	cp := &ConnectionPool{
		connections:  make(map[string]*connItem),
		idleTimeout:  idleTimeout,
		cleanupDelay: cleanupDelay,
	}
	go cp.evictIdleConnections()
	return cp
}

// GetConn retrieves an existing connection or creates a new one if needed.
// It also updates the lastUsed time on every access.
func (p *ConnectionPool) GetConn(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	item, exists := p.connections[addr]
	if exists {
		// Update last used time.
		item.lastUsed = time.Now()
		p.mu.Unlock()
		return item.conn, nil
	}
	p.mu.Unlock()

	// Dial a new connection if one doesn't exist.
	conn, err := grpc.NewClient(addr,
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			// Ensure that s is non-empty.
			if s == "" {
				return nil, fmt.Errorf("empty address provided")
			}
			return net.Dial("tcp", s)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal("Failed to create gRPC client", zap.Error(err))
	}

	// Force connection start:
	conn.Connect()

	p.mu.Lock()
	// Double-check: another goroutine might have created the connection.
	if existing, exists := p.connections[addr]; exists {
		p.mu.Unlock()
		conn.Close()
		return existing.conn, nil
	}
	p.connections[addr] = &connItem{
		conn:     conn,
		lastUsed: time.Now(),
	}
	p.mu.Unlock()

	return conn, nil
}

// evictIdleConnections runs in a background goroutine to close idle connections.
func (p *ConnectionPool) evictIdleConnections() {
	ticker := time.NewTicker(p.cleanupDelay)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		p.mu.Lock()
		for addr, item := range p.connections {
			if now.Sub(item.lastUsed) > p.idleTimeout {
				item.conn.Close()
				delete(p.connections, addr)
			}
		}
		p.mu.Unlock()
	}
}
