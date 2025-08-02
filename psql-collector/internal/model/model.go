package model

import (
	"database/sql"
	"reflect"
	"time"
)

type Knob struct {
	Name   string
	Value  interface{}
	MinVal interface{}
	MaxVal interface{}
}

type ExternalMetric struct {
	Tps     float64
	Latency float64
}

type InternalMetric struct {
	Name  string
	Value interface{}
	Scope Scope
}

type Scope int

const (
	Unspecified Scope = iota
	General
	Table
)

func (s Scope) String() string {
	switch s {
	case General:
		return "general"
	case Table:
		return "table"
	default:
		return "unspecified"
	}
}

func ToScope(scope string) Scope {
	switch scope {
	case "general":
		return General
	case "table":
		return Table
	default:
		return Unspecified
	}
}

type Metric interface {
	IsMetric() bool
}

type QueryTypesDistribution struct {
	Insert int64
	Update int64
	Delete int64
	Select int64
	Other  int64
}

func (t QueryTypesDistribution) IsMetric() bool {
	return true
}

type SharedBufferHitRate struct {
	HitRate float64
}

func (t SharedBufferHitRate) IsMetric() bool {
	return true
}

// TableStat brief information about table
type TableStat struct {
	RelationID   int64
	RelationName string

	NumberOfLiveTuples int64
	NumberOfDeadTuples int64

	NumberOfSeqScans   int64
	NumberOfIndexScans int64

	NumberOfInserts int64
	NumberOfUpdates int64
	NumberOfDeletes int64

	LastVacuumTime     sql.NullTime
	LastAutoVacuumTime sql.NullTime
	LastAnalyzeTime    sql.NullTime
	LastAutoAnalyze    sql.NullTime
}

type AggregateStats struct {
	TotalLiveTuples   int64
	AverageLiveTuples float64

	TotalDeadTuples   int64
	AverageDeadTuples float64

	TotalSeqScans   int64
	TotalIndexScans int64

	TotalInserts int64
	TotalUpdates int64
	TotalDeletes int64

	DeadToLiveRatio     float64
	ScanEfficiency      float64
	UpdateToInsertRatio float64

	AverageDaysSinceLastVacuum float64
	ProportionVacuumed         float64
}

func (t AggregateStats) IsMetric() bool {
	return true
}

func AggregateTableStats(stats []TableStat) AggregateStats {
	var aggr AggregateStats

	var totalTuples, totalModifications int64
	var countVacuumed, countAnalyzed, totalTables int64
	var vacuumDays, analyzeDays int

	for _, stat := range stats {
		aggr.TotalLiveTuples += stat.NumberOfLiveTuples
		aggr.TotalDeadTuples += stat.NumberOfDeadTuples

		aggr.TotalSeqScans += stat.NumberOfSeqScans
		aggr.TotalIndexScans += stat.NumberOfIndexScans

		aggr.TotalInserts += stat.NumberOfInserts
		aggr.TotalUpdates += stat.NumberOfUpdates
		aggr.TotalDeletes += stat.NumberOfDeletes

		if stat.LastVacuumTime.Valid {
			vacuumDays += int(time.Since(stat.LastVacuumTime.Time).Hours() / 24)
			countVacuumed++
		}
		if stat.LastAnalyzeTime.Valid {
			analyzeDays += int(time.Since(stat.LastAnalyzeTime.Time).Hours() / 24)
			countAnalyzed++
		}

		totalTables++
	}

	if totalTables > 0 {
		aggr.AverageLiveTuples = float64(aggr.TotalLiveTuples) / float64(totalTables)
		aggr.AverageDeadTuples = float64(aggr.TotalDeadTuples) / float64(totalTables)
	}

	totalTuples = aggr.TotalLiveTuples + aggr.TotalDeadTuples
	totalModifications = aggr.TotalInserts + aggr.TotalUpdates + aggr.TotalDeletes

	if totalTuples > 0 {
		aggr.DeadToLiveRatio = float64(aggr.TotalDeadTuples) / float64(aggr.TotalLiveTuples)
	}
	if totalModifications > 0 {
		aggr.ScanEfficiency = float64(aggr.TotalSeqScans+aggr.TotalIndexScans) / float64(totalModifications)
		aggr.UpdateToInsertRatio = float64(aggr.TotalUpdates) / float64(aggr.TotalInserts)
	}

	if countVacuumed > 0 {
		aggr.AverageDaysSinceLastVacuum = float64(vacuumDays) / float64(countVacuumed)
		aggr.ProportionVacuumed = float64(countVacuumed) / float64(totalTables)
	}

	return aggr
}

func (t TableStat) IsMetric() bool {
	return true
}

type WalWriteAndFlushStat struct {
	CheckpointsTimed    float64
	CheckpointsReq      float64
	CheckpointWriteTime float64
	CheckpointSyncTime  float64
	BuffersCheckpoint   float64
	BuffersClean        float64
	MaxWrittenClean     float64
	BuffersBackend      float64
	BuffersBackendFsync float64
	BuffersAlloc        float64
}

func (t WalWriteAndFlushStat) IsMetric() bool {
	return true
}

type TableBloating struct {
	TableName        string
	NumOfRows        float64
	BloatInPercent   float64
	BloatInMegabytes float64
	TableSize        float64
}

func (t TableBloating) IsMetric() bool {
	return true
}

type IndexBloating struct {
	TableName        string
	IndexName        string
	BloatInPercent   float64
	BloatInMegabytes float64
	IndexSize        float64
	TableSize        float64
	NumOfIndexScans  float64
}

func (t IndexBloating) IsMetric() bool {
	return true
}

type DatabaseStat struct {
	NumOfTransactionsCommitted        float64
	NumOfTransactionsRollback         float64
	NumbOfDiskBlocksRead              float64
	NumbOfDiskBlocksInBufferCache     float64
	NumOfLiveRowsFetchedBySeqAndIndex float64
	NumOfLiveRowsFetchedByIndex       float64
	NufOfRowsInserted                 float64
	NufOfRowsUpdated                  float64
	NumOfRowsDeleted                  float64
	NumOfConflictCanceled             float64
	NumOfTempFiles                    float64
	TotalAmountOfBytesInTempFiles     float64
	NumOfDeadlocks                    float64
	TimeSpentReadingDataFileBlocks    float64
	TimeSpentWritingDataFileBlocks    float64
	TimeSpentExecutingStatements      float64
	TimeSpendIdleInTransaction        float64
}

func (t DatabaseStat) IsMetric() bool {
	return true
}

func ToInternalMetric[T Metric](metric T, scope Scope) []InternalMetric {
	var internalMetrics []InternalMetric

	val := reflect.ValueOf(metric)
	t := val.Type()

	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := t.Field(i)

		internalMetrics = append(internalMetrics, InternalMetric{
			Name:  typeField.Name,
			Value: valueField.Interface(),
			Scope: scope,
		})
	}
	return internalMetrics
}
