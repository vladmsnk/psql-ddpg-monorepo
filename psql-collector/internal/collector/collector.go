package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"

	"postgresHelper/internal/model"
)

type Collector interface {
	CollectKnobs(ctx context.Context) ([]model.Knob, error)

	CollectQueryTypesDistribution(ctx context.Context) (model.QueryTypesDistribution, model.Scope, error)
	CollectTablesInfo(ctx context.Context) ([]model.TableStat, model.Scope, error)
	CalculateSharedBufferHitRate(ctx context.Context) (model.SharedBufferHitRate, model.Scope, error)
	CollectWalWriteAndFlushStat(ctx context.Context) (model.WalWriteAndFlushStat, model.Scope, error)
	CollectTablesBloat(ctx context.Context) ([]model.TableBloating, model.Scope, error)
	CollectIndexesBloat(ctx context.Context) ([]model.IndexBloating, model.Scope, error)
	CollectDatabaseStat(ctx context.Context, databaseName string) (model.DatabaseStat, model.Scope, error)
	SetKnobs(ctx context.Context, knobs []model.Knob) error
}

type Implementation struct {
	db *sql.DB
}

func NewCollector(db *sql.DB) *Implementation {
	return &Implementation{db: db}
}

func (i *Implementation) SetKnobs(ctx context.Context, knobs []model.Knob) error {

	for _, knob := range knobs {
		query := fmt.Sprintf("ALTER SYSTEM SET %s = %v", knob.Name, knob.Value)
		_, err := i.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("db.ExecContext: %w", err)
		}
	}

	_, err := i.db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		return fmt.Errorf("db.ExecContext: %w", err)
	}

	return nil
}

func (i *Implementation) CollectKnobs(ctx context.Context) ([]model.Knob, error) {
	query := `
SELECT 
	name, setting, vartype, min_val, max_val
From pg_settings
`
	var knobs []model.Knob

	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("i.db.QueryContext: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name, setting, vartype    string
			minv, maxv                sql.NullString
			value, minValue, maxValue interface{}
		)
		err := rows.Scan(&name, &setting, &vartype, &minv, &maxv)
		if err != nil {
			return nil, fmt.Errorf("rows.Scan: %w", err)
		}
		switch vartype {
		case "enum":
			//on current implementation step, does not collect enum settings.
			continue
		case "bool":
			//on current implementation step, does not collect bool settings

			//if setting == "on" {
			//	value = true
			//} else if setting == "off" {
			//	value = false
			//} else {
			//	err = fmt.Errorf("unknown value=%s for name=%s", setting, name)
			//}
			continue
		case "integer", "real":
			value, err = strconv.ParseFloat(setting, 64)
			if maxv.Valid {
				maxValue, err = strconv.ParseFloat(maxv.String, 64)
			}
			if minv.Valid {
				minValue, err = strconv.ParseFloat(minv.String, 64)
			}
		case "string":
			//on current implementation step, does not collect string settings

			//value = setting
			continue
		default:
			err = fmt.Errorf("unknown type=%s for name=%s", vartype, name)
		}
		if err != nil {
			log.Println(err)
			continue
		}
		knobs = append(knobs, model.Knob{
			Name:   name,
			Value:  value,
			MaxVal: maxValue,
			MinVal: minValue,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err: %w", err)
	}

	return knobs, nil
}

func (i *Implementation) CollectQueryTypesDistribution(ctx context.Context) (model.QueryTypesDistribution, model.Scope, error) {
	rows, err := i.db.QueryContext(ctx, SelectQueryTypesDistribution)
	if err != nil {
		return model.QueryTypesDistribution{}, model.Unspecified, fmt.Errorf("i.db.QueryContext: %w", err)
	}
	defer rows.Close()

	var distribution model.QueryTypesDistribution
	for rows.Next() {
		var (
			queryType string
			count     int64
		)

		err := rows.Scan(&queryType, &count)
		if err != nil {
			return model.QueryTypesDistribution{}, model.Unspecified, fmt.Errorf("rows.Scan: %w", err)
		}
		switch queryType {
		case "INSERT":
			distribution.Insert = count
		case "UPDATE":
			distribution.Update = count
		case "DELETE":
			distribution.Delete = count
		case "SELECT":
			distribution.Select = count
		default:
			distribution.Other = count
		}

	}
	if err := rows.Err(); err != nil {
		return model.QueryTypesDistribution{}, model.Unspecified, fmt.Errorf("rows.Err: %w", err)
	}

	return distribution, model.General, nil
}

func (i *Implementation) CollectTablesInfo(ctx context.Context) ([]model.TableStat, model.Scope, error) {
	rows, err := i.db.QueryContext(ctx, SelectTablesInfo)
	if err != nil {
		return nil, model.Unspecified, fmt.Errorf("i.db.QueryContext: %w", err)
	}
	defer rows.Close()

	var tablesInfo []model.TableStat
	for rows.Next() {
		var t model.TableStat
		err := rows.Scan(
			&t.RelationID,
			&t.RelationName,
			&t.NumberOfLiveTuples,
			&t.NumberOfDeadTuples,
			&t.NumberOfSeqScans,
			&t.NumberOfIndexScans,
			&t.NumberOfInserts,
			&t.NumberOfUpdates,
			&t.NumberOfDeletes,
			&t.LastVacuumTime,
			&t.LastAutoVacuumTime,
			&t.LastAnalyzeTime,
			&t.LastAutoAnalyze,
		)
		if err != nil {
			return nil, model.Unspecified, fmt.Errorf("rows.Scan: %w", err)
		}
		//Skip adding info about migration table
		if t.RelationName == "goose_db_version" {
			continue
		}
		tablesInfo = append(tablesInfo, t)
	}
	if err := rows.Err(); err != nil {
		return nil, model.Unspecified, fmt.Errorf("rows.Err: %w", err)
	}

	return tablesInfo, model.Table, nil
}

func (i *Implementation) CalculateSharedBufferHitRate(ctx context.Context) (model.SharedBufferHitRate, model.Scope, error) {
	var ration sql.NullFloat64
	err := i.db.QueryRowContext(ctx, SelectSharedBufferHitRate).Scan(&ration)

	if err != nil {
		return model.SharedBufferHitRate{}, model.Unspecified, fmt.Errorf("row.Scan(): %w", err)
	}

	return model.SharedBufferHitRate{HitRate: ration.Float64}, model.General, nil
}

func (i *Implementation) CollectWalWriteAndFlushStat(ctx context.Context) (model.WalWriteAndFlushStat, model.Scope, error) {
	var stat model.WalWriteAndFlushStat
	rows, err := i.db.QueryContext(ctx, SelectWalWriteAndFlushStat)
	if err != nil {
		return model.WalWriteAndFlushStat{}, model.Unspecified, fmt.Errorf("i.db.QueryContext: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(
			&stat.CheckpointsTimed,
			&stat.CheckpointsReq,
			&stat.CheckpointWriteTime,
			&stat.CheckpointSyncTime,
			&stat.BuffersCheckpoint,
			&stat.BuffersClean,
			&stat.MaxWrittenClean,
			&stat.BuffersBackend,
			&stat.BuffersBackendFsync,
			&stat.BuffersAlloc,
		)
		if err != nil {
			return model.WalWriteAndFlushStat{}, model.Unspecified, fmt.Errorf("row.Scan(): %w", err)
		}
	}

	return stat, model.General, nil
}

func (i *Implementation) CollectTablesBloat(ctx context.Context) ([]model.TableBloating, model.Scope, error) {
	var stats []model.TableBloating

	rows, err := i.db.QueryContext(ctx, SelectTablesBloat)
	if err != nil {
		return nil, model.Unspecified, fmt.Errorf("db.QueryContext: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		stat := model.TableBloating{}

		err := rows.Scan(&stat.TableName, &stat.NumOfRows, &stat.BloatInPercent, &stat.BloatInMegabytes, &stat.TableSize)
		if err != nil {
			return nil, model.Unspecified, fmt.Errorf("rows.Scan: %w", err)
		}

		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, model.Unspecified, fmt.Errorf("rows.Err: %w", err)
	}

	return stats, model.Table, nil
}

func (i *Implementation) CollectIndexesBloat(ctx context.Context) ([]model.IndexBloating, model.Scope, error) {
	var stats []model.IndexBloating

	rows, err := i.db.QueryContext(ctx, SelectIndexesBloat)
	if err != nil {
		return nil, model.Unspecified, fmt.Errorf("db.QueryContext: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		stat := model.IndexBloating{}

		err := rows.Scan(&stat.TableName, &stat.IndexName, &stat.BloatInPercent, &stat.BloatInMegabytes, &stat.IndexSize, &stat.TableSize, &stat.NumOfIndexScans)
		if err != nil {
			return nil, model.Unspecified, fmt.Errorf("rows.Scan: %w", err)
		}

		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, model.Unspecified, fmt.Errorf("rows.Err: %w", err)
	}

	return stats, model.Table, nil
}

func (i *Implementation) CollectDatabaseStat(ctx context.Context, databaseName string) (model.DatabaseStat, model.Scope, error) {
	var stat model.DatabaseStat

	rows, err := i.db.QueryContext(ctx, SelectDatabaseStat, databaseName)
	if err != nil {
		return model.DatabaseStat{}, model.Unspecified, fmt.Errorf("db.QueryContext: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(
			&stat.NumOfTransactionsCommitted,
			&stat.NumOfTransactionsRollback,
			&stat.NumbOfDiskBlocksRead,
			&stat.NumbOfDiskBlocksInBufferCache,
			&stat.NumOfLiveRowsFetchedBySeqAndIndex,
			&stat.NumOfLiveRowsFetchedByIndex,
			&stat.NufOfRowsInserted,
			&stat.NufOfRowsUpdated,
			&stat.NumOfRowsDeleted,
			&stat.NumOfConflictCanceled,
			&stat.NumOfTempFiles,
			&stat.TotalAmountOfBytesInTempFiles,
			&stat.NumOfDeadlocks,
			&stat.TimeSpentReadingDataFileBlocks,
			&stat.TimeSpentWritingDataFileBlocks,
			&stat.TimeSpentExecutingStatements,
			&stat.TimeSpendIdleInTransaction,
		)
		if err != nil {
			return model.DatabaseStat{}, model.Unspecified, fmt.Errorf("rows.Scan: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return model.DatabaseStat{}, model.Unspecified, fmt.Errorf("row.Err: %w", err)
	}

	return stat, model.General, nil
}
