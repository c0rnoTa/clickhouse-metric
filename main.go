package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"log"
	_ "time/tzdata"
)

const (
	configFile = "config.yaml"
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
	rows, err := conn.QueryContext(ctx, config.App.Query)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			count uint64
		)
		if err := rows.Scan(&count); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d", count)
	}

}

func connect(c *Configuration) (*sql.DB, error) {

	connStr := fmt.Sprintf("tcp://%s?dial_timeout=1s%s", c.Database.Addr, getCreds(*c))
	conn, err := sql.Open("clickhouse", connStr)
	if err != nil {
		return nil, err
	}

	if err = conn.PingContext(context.Background()); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func getCreds(cfg Configuration) (creds string) {

	if cfg.Database.Username != "" {
		creds = fmt.Sprintf("&username=%s&password=%s&database=%s", cfg.Database.Username, cfg.Database.Password, cfg.Database.Database)
	}

	return creds
}
