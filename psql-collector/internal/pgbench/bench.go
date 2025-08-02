package pgbench

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"postgresHelper/internal/config"
	"regexp"
	"strconv"
	"strings"
)

type Implementation struct {
	db     *sql.DB
	config config.Config
}

func New(db *sql.DB, config config.Config) *Implementation {
	return &Implementation{db: db, config: config}
}

type Bench interface {
	InitializePgbench(ctx context.Context) error
	RunPgbench(ctx context.Context) (float64, float64, error)
}

func (i *Implementation) InitializePgbench(ctx context.Context) error {
	baseCommand := "pgbench"
	cmd := exec.CommandContext(ctx, baseCommand, i.createPgbenchInitCommand()...)

	cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", i.config.PG.Password))

	log.Println(cmd.String())
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("exec.Command : %w", err)
	}
	return nil
}

func (i *Implementation) RunPgbench(ctx context.Context) (float64, float64, error) {
	baseCommand := "pgbench"
	var stdout, stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, baseCommand, i.createPgbenchLoadCommand()...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", i.config.PG.Password))

	log.Println(cmd.String())

	err := cmd.Run()
	if err != nil {
		return 0, 0, fmt.Errorf("exec.Command : %w", err)
	}

	tps, latency, err := extractMetrics(stdout.String())
	if err != nil {
		return 0, 0, fmt.Errorf("extractMetrics: %w", err)
	}
	return tps, latency, nil
}

func (i *Implementation) createPgbenchLoadCommand() []string {
	var args []string

	if i.config.Pgbench.NumOfClients != 0 {
		args = append(args, "-c", fmt.Sprintf("%d", i.config.Pgbench.NumOfClients))
	}

	if i.config.Pgbench.NumOfThreads != 0 {
		args = append(args, "-j", fmt.Sprintf("%d", i.config.Pgbench.NumOfThreads))
	}

	if i.config.Pgbench.Duration != 0 {
		args = append(args, "-T", fmt.Sprintf("%d", i.config.Pgbench.Duration))
	}

	args = append(args, "-h", fmt.Sprintf("%s", i.config.PG.Host), "-p", fmt.Sprintf("%d", i.config.PG.Port), "-U", i.config.PG.User, i.config.PG.Database)

	return args
}

func (i *Implementation) createPgbenchInitCommand() []string {
	var args []string

	args = append(args, "-i")

	if i.config.Pgbench.Scale != 0 {
		args = append(args, fmt.Sprintf("--scale=%d", i.config.Pgbench.Scale))
	}
	if i.config.Pgbench.ForeignKeys {
		args = append(args, "--foreign-keys")

	}
	if i.config.Pgbench.Partitions != 0 {
		args = append(args, fmt.Sprintf("--partitions=%d", i.config.Pgbench.Scale))
	}

	args = append(args, "-h", fmt.Sprintf("%s", i.config.PG.Host), "-p", fmt.Sprintf("%d", i.config.PG.Port), "-U", i.config.PG.User, i.config.PG.Database)
	return args
}

func extractMetrics(output string) (float64, float64, error) {
	var (
		tps, latency float64
		err          error
	)
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, "tps") {
			tps, err = extractNumber(line)
		}
		if strings.Contains(line, "latency") {
			latency, err = extractNumber(line)
		}
	}
	return tps, latency, err
}

func extractNumber(s string) (float64, error) {
	re := regexp.MustCompile(`\d+\.\d+`)
	match := re.FindString(s)
	if match == "" {
		return 0, fmt.Errorf("no number found in string")
	}
	return strconv.ParseFloat(match, 64)
}
