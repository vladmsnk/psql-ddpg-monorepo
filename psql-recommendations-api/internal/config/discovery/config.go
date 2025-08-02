package discovery

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"psqlRecommendationsApi/cmd"
)

var pathToConfig = "config/config.yaml"

type Config struct {
	Redis               Redis                   `yaml:"redis"`
	DiscoveryGRPCServer cmd.GRPCConfigDiscovery `yaml:"discovery-grpc-server"`
}

type Redis struct {
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
