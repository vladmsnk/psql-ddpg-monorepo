package autovacuum

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Helper interface {
	IsEnabled(ctx context.Context) (bool, error)
	ReadCurrentAutovacuumSettings(ctx context.Context) (VacuumStats, error)
	FindLongRunningTransactions(ctx context.Context)
}

type Impl struct {
	db          *sql.DB
	queryTimout time.Duration
}

func New(db *sql.DB) *Impl {
	return &Impl{db: db, queryTimout: 5 * time.Second}
}

func (i *Impl) IsEnabled(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, i.queryTimout)
	defer cancel()

	query := `
	SELECT setting FROM pg_settings WHERE name = ANY (ARRAY ['autovacuum', 'track_counts']);
`
	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return false, fmt.Errorf("i.db.Query: %v", err)
	}
	defer rows.Close()

	iter := 0
	var autovacuum, trackCounts string
	for rows.Next() {
		if iter == 0 {
			err = rows.Scan(&autovacuum)
		} else {
			err = rows.Scan(&trackCounts)
		}
		if err != nil {
			return false, fmt.Errorf("rows.Scan: %v", err)
		}
		iter++
	}
	return autovacuum == "on" && trackCounts == "on", nil
}

func (i *Impl) ReadCurrentAutovacuumSettings(ctx context.Context) (VacuumStats, error) {
	ctx, cancel := context.WithTimeout(ctx, i.queryTimout)
	defer cancel()

	query := `
	SELECT name, setting FROM pg_settings WHERE category = 'Autovacuum' and name != 'autovacuum';
`
	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return VacuumStats{}, fmt.Errorf("i.db.QueryContext: %v", err)
	}
	defer rows.Close()

	var settings []SettingDto
	for rows.Next() {
		t := SettingDto{}
		err := rows.Scan(&t.name, &t.setting)
		if err != nil {
			return VacuumStats{}, fmt.Errorf("rows.Scan: %v", err)
		}

		settings = append(settings, t)
	}
	return ToVacuumStats(settings)
}
