package selector

import (
	"context"
	"fmt"
	"slices"

	"postgresHelper/internal/config"
	"postgresHelper/internal/model"
)

type Selector interface {
	ListAllMetrics(ctx context.Context) ([]model.InternalMetric, error)
	ListAllAggregatedMetrics(ctx context.Context) ([]model.InternalMetric, error)
	ListKnobs(ctx context.Context) ([]model.Knob, error)
}

type MetricCollector interface {
	CollectQueryTypesDistribution(ctx context.Context) (model.QueryTypesDistribution, model.Scope, error)
	CollectTablesInfo(ctx context.Context) ([]model.TableStat, model.Scope, error)
	CalculateSharedBufferHitRate(ctx context.Context) (model.SharedBufferHitRate, model.Scope, error)
	CollectWalWriteAndFlushStat(ctx context.Context) (model.WalWriteAndFlushStat, model.Scope, error)
	CollectTablesBloat(ctx context.Context) ([]model.TableBloating, model.Scope, error)
	CollectIndexesBloat(ctx context.Context) ([]model.IndexBloating, model.Scope, error)
	CollectDatabaseStat(ctx context.Context, databaseName string) (model.DatabaseStat, model.Scope, error)
	CollectKnobs(ctx context.Context) ([]model.Knob, error)
}

func New(c MetricCollector, config config.Postgres) *Implementation {
	return &Implementation{c: c, config: config}
}

type Implementation struct {
	c      MetricCollector
	config config.Postgres
}

func (i *Implementation) listAggregatedTableBloatMetrics(ctx context.Context) ([]model.InternalMetric, error) {

	var tableBloatAggregatedMetrics []model.InternalMetric
	tableBloatStats, scope, err := i.c.CollectTablesBloat(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectTablesBloat: %w", err)
	}

	if len(tableBloatStats) == 0 {
		return nil, fmt.Errorf("no table bloat stats")
	}

	statContainingMaximimBloat := slices.MaxFunc(tableBloatStats, func(a, b model.TableBloating) int {
		if a.BloatInPercent < b.BloatInPercent {
			return -1
		} else if a.BloatInPercent > b.BloatInPercent {
			return 1
		}
		return 0
	})
	tableBloatAggregatedMetrics = append(tableBloatAggregatedMetrics, model.ToInternalMetric(statContainingMaximimBloat, scope)...)

	return tableBloatAggregatedMetrics, nil
}

func (i *Implementation) listAggregatedIndexBloatMetrics(ctx context.Context) ([]model.InternalMetric, error) {
	var indexBloatAggregatedMetrics []model.InternalMetric
	tableIndexBloatStats, scope, err := i.c.CollectIndexesBloat(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectIndexesBloat: %w", err)
	}

	if len(tableIndexBloatStats) == 0 {
		return nil, fmt.Errorf("no index bloat stats")
	}

	statContainingMaximimBloat := slices.MaxFunc(tableIndexBloatStats, func(a, b model.IndexBloating) int {
		if a.BloatInPercent < b.BloatInPercent {
			return -1
		} else if a.BloatInPercent > b.BloatInPercent {
			return 1
		}
		return 0
	})
	indexBloatAggregatedMetrics = append(indexBloatAggregatedMetrics, model.ToInternalMetric(statContainingMaximimBloat, scope)...)

	return indexBloatAggregatedMetrics, nil
}

func (i *Implementation) listAggregatedTableInfoMetrics(ctx context.Context) ([]model.InternalMetric, error) {
	var tablesInfoAggregatedMetrics []model.InternalMetric

	tablesInfoMetrics, scope, err := i.c.CollectTablesInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("i.c.CollectTablesInfo: %w", err)
	}

	aggregatedTableInfoMetrics := model.AggregateTableStats(tablesInfoMetrics)
	tablesInfoAggregatedMetrics = append(tablesInfoAggregatedMetrics, model.ToInternalMetric(aggregatedTableInfoMetrics, scope)...)

	return tablesInfoAggregatedMetrics, nil
}

func (i *Implementation) ListAllAggregatedMetrics(ctx context.Context) ([]model.InternalMetric, error) {
	var metrics []model.InternalMetric

	databaseStat, scope, err := i.c.CollectDatabaseStat(ctx, i.config.Database)
	if err != nil {
		return nil, fmt.Errorf("c.CollectDatabaseStat: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(databaseStat, scope)...)

	walWriteStat, scope, err := i.c.CollectWalWriteAndFlushStat(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectWalWriteAndFlushStat: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(walWriteStat, scope)...)

	sharedBufferHitRate, scope, err := i.c.CalculateSharedBufferHitRate(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CalculateSharedBufferHitRate: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(sharedBufferHitRate, scope)...)

	distribution, scope, err := i.c.CollectQueryTypesDistribution(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectQueryTypesDistribution: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(distribution, scope)...)

	aggregatedIndexBloat, err := i.listAggregatedIndexBloatMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("i.ListAggregatedIndexBloatMetrics: %w", err)
	}
	metrics = append(metrics, aggregatedIndexBloat...)

	aggregatedTableBloat, err := i.listAggregatedTableBloatMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("i.ListAggregatedIndexBloatMetrics: %w", err)
	}
	metrics = append(metrics, aggregatedTableBloat...)

	aggregatedTablesInfo, err := i.listAggregatedTableInfoMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("i.ListAggregatedIndexBloatMetrics: %w", err)
	}
	metrics = append(metrics, aggregatedTablesInfo...)

	return metrics, nil
}

func (i *Implementation) ListAllMetrics(ctx context.Context) ([]model.InternalMetric, error) {
	var metrics []model.InternalMetric

	databaseStat, scope, err := i.c.CollectDatabaseStat(ctx, i.config.Database)
	if err != nil {
		return nil, fmt.Errorf("c.CollectDatabaseStat: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(databaseStat, scope)...)

	tableInfoStats, scope, err := i.c.CollectTablesInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectTablesInfo: %w", err)
	}
	for _, stat := range tableInfoStats {
		metrics = append(metrics, model.ToInternalMetric(stat, scope)...)
	}

	walWriteStat, scope, err := i.c.CollectWalWriteAndFlushStat(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectWalWriteAndFlushStat: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(walWriteStat, scope)...)

	indexBloatStats, scope, err := i.c.CollectIndexesBloat(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectIndexesBloat: %w", err)
	}
	for _, stat := range indexBloatStats {
		metrics = append(metrics, model.ToInternalMetric(stat, scope)...)
	}

	tableBloatStats, scope, err := i.c.CollectTablesBloat(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectTablesBloat: %w", err)
	}
	for _, stat := range tableBloatStats {
		metrics = append(metrics, model.ToInternalMetric(stat, scope)...)
	}

	sharedBufferHitRate, scope, err := i.c.CalculateSharedBufferHitRate(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CalculateSharedBufferHitRate: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(sharedBufferHitRate, scope)...)

	distribution, scope, err := i.c.CollectQueryTypesDistribution(ctx)
	if err != nil {
		return nil, fmt.Errorf("c.CollectQueryTypesDistribution: %w", err)
	}
	metrics = append(metrics, model.ToInternalMetric(distribution, scope)...)

	return metrics, nil
}

func (i *Implementation) ListKnobs(ctx context.Context) ([]model.Knob, error) {
	knobs, err := i.c.CollectKnobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("i.c.CollectKnobs: %w", err)
	}
	return knobs, nil
}
