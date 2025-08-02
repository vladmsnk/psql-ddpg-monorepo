package environment

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"psqlRecommendationsApi/cmd"
)

const pathToConfig = "config/config.yaml"

type Config struct {
	DiscoveryClient       DiscoveryClient           `yaml:"discovery-grpc-client"`
	EnvironmentGRPCServer cmd.GRPCConfigEnvironment `yaml:"grpc"`
	CollectorClient       CollectorClient           `yaml:"collector"`
}

type CollectorClient struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type DiscoveryClient struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

var ConfigStruct Config

func Init() error {
	rawYaml, err := os.ReadFile(pathToConfig)
	if err != nil {
		return fmt.Errorf("os.ReadFile: %w", err)
	}

	if err = yaml.Unmarshal(rawYaml, &ConfigStruct); err != nil {
		return fmt.Errorf("yaml.Unmarshal: %w", err)
	}
	return nil
}
