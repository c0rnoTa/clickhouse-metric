package main

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Configuration struct {
	Database struct {
		Addr     string `yaml:"addr"`
		Database string `yaml:"database"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"clickhouse"`
	App struct {
		Query string `yaml:"query"`
	} `yaml:"app"`
}

// Load file contents to Configuration object.
func (c *Configuration) Load(configFile string) error {

	file, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(file, c)
}
