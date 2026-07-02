package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	LogLevel string `yaml:"log-level"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	err = yaml.Unmarshal(b, &c)
	return &c, err
}
