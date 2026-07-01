package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Multiaddrs []string `yaml:"multiaddrs"`
	FileDir    string   `yaml:"file-dir"`
	DBPath     string   `yaml:"db-path"`
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
