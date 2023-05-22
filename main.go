package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	configFile              = "config.yaml"
	clickhouseClientName    = "an-example-go-client"
	clickhouseClientVersion = "0.1"
)

func main() {
	var config Configuration

	// Подгружаем конфигурацию из файла
	if err := config.Load(configFile); err != nil {
		log.Fatalf("Fail to load config file: %v", err)
	}

	conn, err := connect(&config)
	if err != nil {
		log.Fatalf("Fail to connect: %v", err)
	}

	ctx := context.Background()
	rows, err := conn.Query(ctx, config.App.Query)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			name, uuid string
		)
		if err := rows.Scan(
			&name,
			&uuid,
		); err != nil {
			log.Fatal(err)
		}
		log.Printf("name: %s, uuid: %s",
			name, uuid)
	}

}

func connect(c *Configuration) (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{c.Database.Addr},
			Auth: clickhouse.Auth{
				Database: c.Database.Database,
				Username: c.Database.Username,
				Password: c.Database.Password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: clickhouseClientName, Version: clickhouseClientVersion},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
