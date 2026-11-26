package importhttp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// closeTimeout is how long Close waits for the requests in flight to
// return before it closes their connections
const closeTimeout = 5 * time.Second

// readHeaderTimeout is how long a Server waits for a request's headers. A
// part's response streams for as long as the part takes, so the server has
// no write timeout.
const readHeaderTimeout = time.Minute

// Server is an HTTP server of a sender (see NewSender) that stops its
// requests when it closes, so the sender's stores can be closed after it
type Server struct {
	server   *http.Server
	listener net.Listener
	// cancel cancels the context of every request
	cancel context.CancelFunc
	// closeTimeout is how long Close waits for the requests in flight
	closeTimeout time.Duration

	// mu guards closing, so that no request starts once Close waits
	mu       sync.Mutex
	closing  bool
	requests sync.WaitGroup
}

// Listen returns a server of handler that listens on addr, a "host:port".
// It serves once Serve is called.
func Listen(addr string, handler http.Handler) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		listener:     l,
		cancel:       cancel,
		closeTimeout: closeTimeout,
	}
	s.server = &http.Server{
		Handler:           s.track(handler),
		ReadHeaderTimeout: readHeaderTimeout,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	return s, nil
}

// track counts the requests to h, and refuses them with 503 once the
// server is closing
func (s *Server) track(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			http.Error(w, "the sender is stopping", http.StatusServiceUnavailable)
			return
		}
		s.requests.Add(1)
		s.mu.Unlock()
		defer s.requests.Done()
		h.ServeHTTP(w, r)
	})
}

// Addr returns the address that the server listens on
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// URL returns the server's base URL
func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

// Serve serves requests until Close, and then returns http.ErrServerClosed
func (s *Server) Serve() error {
	log.Infow("Import sender listening", "addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

// Close stops the server. It cancels the requests in flight, which stop
// before their next entry. A request blocked on a reader that does not read
// is stopped after closeTimeout by closing its connection, which fails its
// write. Close returns once every request has returned, so the sender's
// stores can be closed after it. The readers request those parts again.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), s.closeTimeout)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if err != nil {
		log.Warnw("Import requests did not stop in time, closing their connections", "err", err)
		err = s.server.Close()
	}
	s.requests.Wait()
	return err
}
