package multiplex

import (
	"context"
	"fmt"
	"sync"

	dbm "github.com/cometbft/cometbft-db"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
)

// DBService defines a registry for open database connections.
type DBService struct {
	service.BaseService
	mtx *sync.Mutex

	name    string
	backend dbm.BackendType // string
	storage string

	// db will be nil on closed or reset.
	db dbm.DB

	// Options
	logger cmtlog.Logger
}

type DBServiceOption func(*DBService)

// NewDBService creates a new database service.
func NewDBService(
	ctx context.Context,
	dbName string,
	storagePath string,
	backendType string,
	logger cmtlog.Logger,
	options ...DBServiceOption,
) *DBService {
	dbs := &DBService{
		mtx: new(sync.Mutex),

		name:    dbName,
		storage: storagePath,
		backend: dbm.BackendType(backendType),

		// Options
		logger: logger,
	}

	// Use option helpers
	dbs.SetOptions(options...)

	dbs.BaseService = *service.NewBaseService(ctx, nil, "DBService", dbs)
	return dbs
}

// DBServiceWithLogger injects a custom logger instance.
func DBServiceWithLogger(logger cmtlog.Logger) DBServiceOption {
	return func(dbs *DBService) {
		dbs.logger = logger
	}
}

// ----------------------------------------------------------------------------
// DBService implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (dbS *DBService) OnStart(ctx context.Context) (err error) {
	// TODO(midas): remove debug logs
	dbS.logger.Debug("Starting database",
		"name", dbS.name,
		"type", dbS.backend,
		"path", dbS.storage,
	)

	dbS.mtx.Lock()
	defer dbS.mtx.Unlock()

	if dbS.db, err = dbm.NewDB(
		dbS.name,
		dbS.backend,
		dbS.storage,
	); err != nil {
		return fmt.Errorf(
			"failed to open database %s: %w", dbS.name, err)
	}

	return nil
}

// OnStop implements [service.Service] by closing the database.
func (dbS *DBService) OnStop() {
	dbS.mtx.Lock()
	defer dbS.mtx.Unlock()

	if err := dbS.db.Close(); err != nil {
		dbS.logger.Error("failed to close database",
			"name", dbS.name,
			"path", dbS.storage,
			"err", err,
		)
	}
}

// OnReset implements [service.Service] by resetting the service.
func (dbS *DBService) OnReset() error {
	dbS.mtx.Lock()
	dbS.db = nil
	dbS.mtx.Unlock()

	dbS.logger.Debug("Reset database service", "name", dbS.name)
	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers to configure a ReplayPool instance.
func (dbS *DBService) SetOptions(options ...DBServiceOption) {
	for _, option := range options {
		option(dbS)
	}
}

// Logger returns the logger instance.
func (dbS *DBService) Logger() cmtlog.Logger {
	return dbS.logger
}

// Name returns the database name
func (dbS *DBService) Name() string {
	return dbS.name
}

// Path returns the storage path.
func (dbS *DBService) Path() string {
	return dbS.storage
}

// Type returns the backend type.
func (dbS *DBService) Type() dbm.BackendType {
	return dbS.backend
}

// DB returns the database adapter.
func (dbS *DBService) DB() dbm.DB {
	dbS.mtx.Lock()
	defer dbS.mtx.Unlock()

	return dbS.db
}
