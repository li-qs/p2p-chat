package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Port    int      `yaml:"port"`
	Bind    []string `yaml:"bind"`
	FileDir string   `yaml:"file_dir"`
}

var c Config

func Get() Config {
	return c
}

func Init(path string) {
	err := LoadConfig(path, &c)
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
