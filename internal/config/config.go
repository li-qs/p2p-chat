package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func LoadConfig(filepath string, c *Config) error {
	b, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(b, c)
}
