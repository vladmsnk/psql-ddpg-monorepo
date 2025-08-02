package autovacuum

import (
	"database/sql"
	"time"
)

type VacuumStatsEntry struct {
	SchemaName   string
	RelationName string

	LiveRowCount int32
	DeadRowCount int32
	Relfrozenxid int32
	Relminmxid   int32

	LastManualVacuumRun  sql.Null[time.Time]
	LastAutoVacuumRun    sql.Null[time.Time]
	LastManualAnalyzeRun sql.Null[time.Time]
	LastAutoAnalyzeRun   sql.Null[time.Time]

	AutovacuumEnabled               bool
	AutovacuumVacuumThreshold       int32
	AutovacuumAnalyzeThreshold      int32
	AutovacuumVacuumScaleFactor     float64
	AutovacuumAnalyzeScaleFactor    float64
	AutovacuumFreezeMaxAge          int32
	AutovacuumMultixactFreezeMaxAge int32
	AutovacuumVacuumCostDelay       int32
	AutovacuumVacuumCostLimit       int32

	Fillfactor int32
}

type VacuumStats struct {
	DatabaseName string

	AutovacuumMaxWorkers     int32
	AutovacuumWorkMem        int32
	AutovacuumNaptimeSeconds int32

	AutovacuumEnabled               bool
	AutovacuumVacuumThreshold       int32
	AutovacuumAnalyzeThreshold      int32
	AutovacuumVacuumScaleFactor     float64
	AutovacuumAnalyzeScaleFactor    float64
	AutovacuumFreezeMaxAge          int32
	AutovacuumMultixactFreezeMaxAge int32
	AutovacuumVacuumCostDelay       int32
	AutovacuumVacuumCostLimit       int32

	Relations []VacuumStatsEntry
}
