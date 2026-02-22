package main

import (
	"sync"
)

type HandleRegistry struct {
	mu           sync.RWMutex
	nextID       uintptr
	environments map[uintptr]*Environment
	connections  map[uintptr]*Connection
	statements   map[uintptr]*Statement
}

var registry = &HandleRegistry{
	nextID:       1,
	environments: make(map[uintptr]*Environment),
	connections:  make(map[uintptr]*Connection),
	statements:   make(map[uintptr]*Statement),
}

func (r *HandleRegistry) allocEnv() (uintptr, *Environment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	env := &Environment{}
	r.environments[id] = env
	return id, env
}

func (r *HandleRegistry) getEnv(id uintptr) *Environment {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.environments[id]
}

func (r *HandleRegistry) freeEnv(id uintptr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.environments, id)
}

func (r *HandleRegistry) allocConn(envID uintptr) (uintptr, *Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	conn := &Connection{envID: envID}
	r.connections[id] = conn
	return id, conn
}

func (r *HandleRegistry) getConn(id uintptr) *Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.connections[id]
}

func (r *HandleRegistry) freeConn(id uintptr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.connections, id)
}

func (r *HandleRegistry) allocStmt(connID uintptr) (uintptr, *Statement) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	stmt := &Statement{connID: connID}
	r.statements[id] = stmt
	return id, stmt
}

func (r *HandleRegistry) getStmt(id uintptr) *Statement {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.statements[id]
}

func (r *HandleRegistry) freeStmt(id uintptr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.statements, id)
}
