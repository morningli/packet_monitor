package redis

import (
	"sync"
	"time"
)

type Session struct {
	address  string
	in       *Decoder
	out      *Decoder
	lastTime time.Time
	mux      sync.Mutex
}

func NewSession(address string) *Session {
	return &Session{address: address, in: NewDecoder(true), out: NewDecoder(false), lastTime: time.Now()}
}

func (s *Session) AppendAndFetch(data []byte, in bool) (ret []Resp) {
	s.mux.Lock()
	defer s.mux.Unlock()

	d := s.in
	if !in {
		d = s.out
	}
	s.lastTime = time.Now()

	d.Append(data)
	for {
		args := d.TryDecode()
		if !args.Valid() {
			break
		}
		ret = append(ret, args)
	}
	return
}

type SessionMgr struct {
	mux      sync.RWMutex
	sessions map[string]*Session
}

const sessionTimeout = time.Minute * 30

func NewSessionMgr() *SessionMgr {
	mgr := &SessionMgr{sessions: map[string]*Session{}}
	go func() {
		tick := time.NewTicker(time.Minute * 5)
		defer tick.Stop()
		for {
			_, ok := <-tick.C
			if !ok {
				return
			}

			var expireSessions []string

			mgr.mux.RLock()
			i := 500
			for address, v := range mgr.sessions {
				if time.Since(v.lastTime) > sessionTimeout {
					expireSessions = append(expireSessions, address)
				}
				i--
				if i == 0 {
					break
				}
			}
			mgr.mux.RUnlock()

			mgr.mux.Lock()
			for _, addr := range expireSessions {
				delete(mgr.sessions, addr)
			}
			mgr.mux.Unlock()
		}
	}()
	return mgr
}

func (s *SessionMgr) session(address string) *Session {
	s.mux.RLock()
	session, ok := s.sessions[address]
	if ok {
		s.mux.RUnlock()
		return session
	}
	s.mux.RUnlock()

	s.mux.Lock()
	defer s.mux.Unlock()

	session, ok = s.sessions[address]
	if ok {
		return session
	}
	session = NewSession(address)
	s.sessions[address] = session
	return session
}

func (s *SessionMgr) AppendAndFetch(address string, data []byte, in bool) []Resp {
	session := s.session(address)
	return session.AppendAndFetch(data, in)
}
