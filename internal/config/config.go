package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Port     int      `yaml:"port"`
	Bind     []string `yaml:"bind"`
	CacheDir string   `yaml:"cache_dir"`
}

var Conf Config

func InitConfig(path string) {
	err := LoadConfig(path, &Conf)
	if err != nil {
		panic(err)
	}
}

func LoadConfig(path string, c *Config) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(b, c)
}
