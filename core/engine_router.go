package core

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// EngineFactoryFunc creates a fully configured Engine for a project.
// sourceProjectName is the name of the project that triggered the creation
// (used to inherit settings like admin_from).
// Implemented in cmd/cc-connect/main.go where all wiring is available.
type EngineFactoryFunc func(projectName, workDir, agentType string, sourceProjectName string, platformConfigs []PlatformInheritConfig) (*Engine, error)

// PlatformInheritConfig describes a platform configuration to inherit when creating a new project.
type PlatformInheritConfig struct {
	Type    string
	Options map[string]any
}

// projectBindingData is the on-disk format for session→project bindings.
type projectBindingData struct {
	Bindings map[string]string `json:"bindings,omitempty"`
}

// EngineRouter routes messages from platforms to the correct Engine based on
// per-session project bindings. It enables /project switch at runtime.
type EngineRouter struct {
	mu       sync.RWMutex
	engines  map[string]*Engine // projectName → Engine
	bindings map[string]string  // sessionKey → projectName

	storePath string // persistence path for bindings
	factory   EngineFactoryFunc

	// Callbacks for external systems (cron, heartbeat, bridge, management, etc.)
	onEngineCreated   []func(name string, e *Engine)
	onEngineDestroyed []func(name string)

	// Callback to remove project from config file when destroying an engine
	projectRemoveFunc func(name string) error
}

// NewEngineRouter creates a new router and loads any persisted bindings.
func NewEngineRouter(dataDir string) *EngineRouter {
	r := &EngineRouter{
		engines:   make(map[string]*Engine),
		bindings:  make(map[string]string),
		storePath: filepath.Join(dataDir, "project_bindings.json"),
	}
	r.load()
	return r
}

// SetFactory registers the Engine creation function.
func (r *EngineRouter) SetFactory(fn EngineFactoryFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factory = fn
}

// OnEngineCreated registers a callback invoked after a new Engine is created and started.
func (r *EngineRouter) OnEngineCreated(fn func(name string, e *Engine)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onEngineCreated = append(r.onEngineCreated, fn)
}

// OnEngineDestroyed registers a callback invoked before an Engine is stopped and removed.
func (r *EngineRouter) OnEngineDestroyed(fn func(name string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onEngineDestroyed = append(r.onEngineDestroyed, fn)
}

// SetProjectRemoveFunc sets the callback for removing a project from the config file.
func (r *EngineRouter) SetProjectRemoveFunc(fn func(name string) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.projectRemoveFunc = fn
}

// RegisterEngine adds an already-started Engine to the router.
func (r *EngineRouter) RegisterEngine(name string, e *Engine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.engines[name] = e
	slog.Info("engine_router: registered engine", "project", name)
}

// UnregisterEngine removes an Engine from the router.
func (r *EngineRouter) UnregisterEngine(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.engines, name)
	// Clean up bindings pointing to this project
	for sk, proj := range r.bindings {
		if proj == name {
			delete(r.bindings, sk)
		}
	}
	r.saveLocked()
	slog.Info("engine_router: unregistered engine", "project", name)
}

// HandleMessage is the MessageHandler passed to Platform.Start.
// It routes the message to the correct Engine based on session binding.
func (r *EngineRouter) HandleMessage(p Platform, msg *Message) {
	r.mu.RLock()
	projectName := r.bindings[msg.SessionKey]
	var target *Engine
	if projectName != "" {
		target = r.engines[projectName]
	}
	r.mu.RUnlock()

	if target != nil {
		target.handleMessage(p, msg)
		return
	}

	// No binding or target not found — fall back to the first Engine
	// that owns this platform (the original project for the platform).
	r.mu.RLock()
	for _, e := range r.engines {
		for _, ep := range e.platforms {
			if ep == p {
				r.mu.RUnlock()
				e.handleMessage(p, msg)
				return
			}
		}
	}
	r.mu.RUnlock()

	slog.Warn("engine_router: no engine found for message", "session_key", msg.SessionKey, "platform", p.Name())
}

// Bind associates a session with a project. Subsequent messages from this
// session will be routed to the named project's Engine.
func (r *EngineRouter) Bind(sessionKey, projectName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.engines[projectName]; !ok {
		return ErrProjectNotFound
	}
	r.bindings[sessionKey] = projectName
	r.saveLocked()
	slog.Info("engine_router: bound session to project", "session_key", sessionKey, "project", projectName)
	return nil
}

// Unbind removes a session's project binding.
func (r *EngineRouter) Unbind(sessionKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.bindings, sessionKey)
	r.saveLocked()
}

// GetBinding returns the project name bound to a session, or "" if none.
func (r *EngineRouter) GetBinding(sessionKey string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bindings[sessionKey]
}

// ListProjects returns the names of all registered projects.
func (r *EngineRouter) ListProjects() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.engines))
	for name := range r.engines {
		names = append(names, name)
	}
	return names
}

// GetEngine returns the Engine for a project, or nil.
func (r *EngineRouter) GetEngine(name string) *Engine {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.engines[name]
}

// CreateEngine uses the factory to create, start, and register a new Engine.
func (r *EngineRouter) CreateEngine(projectName, workDir, agentType, sourceProjectName string, platformConfigs []PlatformInheritConfig) (*Engine, error) {
	r.mu.Lock()
	factory := r.factory
	r.mu.Unlock()

	if factory == nil {
		return nil, ErrProjectFactoryNotSet
	}

	e, err := factory(projectName, workDir, agentType, sourceProjectName, platformConfigs)
	if err != nil {
		return nil, err
	}

	// Every engine managed by the router must have a reference to it
	// so that /project commands work after switching projects.
	e.SetRouter(r)

	if err := e.Start(); err != nil {
		slog.Warn("engine_router: new engine start partially failed", "project", projectName, "error", err)
	}

	r.mu.Lock()
	r.engines[projectName] = e
	callbacks := make([]func(string, *Engine), len(r.onEngineCreated))
	copy(callbacks, r.onEngineCreated)
	r.mu.Unlock()

	for _, cb := range callbacks {
		cb(projectName, e)
	}

	slog.Info("engine_router: created and registered engine", "project", projectName)
	return e, nil
}

// DestroyEngine stops and removes an Engine from the router.
func (r *EngineRouter) DestroyEngine(name string) error {
	r.mu.Lock()
	e, ok := r.engines[name]
	if !ok {
		r.mu.Unlock()
		return ErrProjectNotFound
	}
	callbacks := make([]func(string), len(r.onEngineDestroyed))
	copy(callbacks, r.onEngineDestroyed)
	removeFunc := r.projectRemoveFunc
	r.mu.Unlock()

	// Notify callbacks before stopping
	for _, cb := range callbacks {
		cb(name)
	}

	e.Stop()

	// Remove from config file
	if removeFunc != nil {
		if err := removeFunc(name); err != nil {
			slog.Warn("engine_router: failed to remove project from config", "project", name, "error", err)
		}
	}

	r.mu.Lock()
	delete(r.engines, name)
	// Clean up bindings
	for sk, proj := range r.bindings {
		if proj == name {
			delete(r.bindings, sk)
		}
	}
	r.saveLocked()
	r.mu.Unlock()

	slog.Info("engine_router: destroyed engine", "project", name)
	return nil
}

// Errors
var (
	ErrProjectNotFound      = EngineError("project not found")
	ErrProjectAlreadyExists = EngineError("project already exists")
	ErrProjectFactoryNotSet = EngineError("project factory not set")
)

type EngineError string

func (e EngineError) Error() string { return string(e) }

// --- Persistence ---

func (r *EngineRouter) saveLocked() {
	if r.storePath == "" {
		return
	}
	data := projectBindingData{Bindings: r.bindings}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		slog.Error("engine_router: failed to marshal bindings", "error", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(r.storePath), 0o755); err != nil {
		slog.Error("engine_router: failed to create dir", "path", r.storePath, "error", err)
		return
	}
	if err := AtomicWriteFile(r.storePath, b, 0o644); err != nil {
		slog.Error("engine_router: failed to write bindings", "path", r.storePath, "error", err)
	}
}

func (r *EngineRouter) load() {
	if r.storePath == "" {
		return
	}
	data, err := os.ReadFile(r.storePath)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("engine_router: failed to read bindings", "path", r.storePath, "error", err)
		}
		return
	}
	var d projectBindingData
	if err := json.Unmarshal(data, &d); err != nil {
		slog.Error("engine_router: failed to unmarshal bindings", "path", r.storePath, "error", err)
		return
	}
	if d.Bindings != nil {
		r.bindings = d.Bindings
	}
}
