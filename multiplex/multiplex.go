package multiplex

// ----------------------------------------------------------------------------
// FILESYSTEM

// MultiplexGroup maps scope hashes to autofile group instances
type MultiplexGroup map[string]*ScopedGroup

// MultiplexWAL maps scope hashes to scoped WAL instances
type MultiplexWAL map[string]*ScopedWAL

