package config

import (
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"postgresHelper/lib/grpc_server"
)

const pathToConfig = "config/config.yaml"

type Config struct {
	GRPC    grpc_server.GRPCConfig `yaml:"grpc"`
	PG      Postgres               `yaml:"postgres"`
	Pgbench Pgbench                `yaml:"pgbench"`
}

type Postgres struct {
	Host          string `envconfig:"HOST"`
	Port          int    `envconfig:"PORT"`
	User          string `envconfig:"USER"`
	Password      string `envconfig:"PASSWORD"`
	Database      string `envconfig:"DATABASE"`
	SSLMode       string `envconfig:"SSLMODE"`
	ContainerName string `envconfig:"CONTAINER_NAME"`
}

type Pgbench struct {
	NumOfClients int64  `yaml:"num_of_clients"`
	NumOfThreads int64  `yaml:"num_of_threads"`
	Duration     int64  `yaml:"duration"`
	Database     string `yaml:"database"`
	Partitions   int64  `yaml:"partitions"`
	NoVacuum     bool   `yaml:"no_vacuum"`
	Scale        int64  `yaml:"scale"`
	ForeignKeys  bool   `yaml:"foreign_keys"`
}

func (pg *Postgres) ConnectionString() string {
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		pg.Host, pg.Port, pg.User, pg.Password, pg.Database, pg.SSLMode)
	log.Println(conn)
	return conn
}

var ConfigStruct Config

func Init() error {

	// Read config file for benchmark
	rawYaml, err := os.ReadFile(pathToConfig)
	if err != nil {
		return fmt.Errorf("os.ReadFile: %w", err)
	}
	if err = yaml.Unmarshal(rawYaml, &ConfigStruct); err != nil {
		return fmt.Errorf("yaml.Unmarshal: %w", err)
	}

	p := &ConfigStruct.PG
	err = envconfig.Process("PG", p)
	if err != nil {
		return fmt.Errorf("envconfig.Process: %w", err)
	}

	return nil
}
