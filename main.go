package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
)

const (
	configFile = "config.yaml"
	//clickhouseClientName    = "an-example-go-client"
	//clickhouseClientVersion = "0.1"
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
	//var (
	//	ctx  = context.Background()
	//	conn = clickhouse.OpenDB(&clickhouse.Options{
	//		Addr: []string{c.Database.Addr},
	//		Auth: clickhouse.Auth{
	//			Database: c.Database.Database,
	//			Username: c.Database.Username,
	//			Password: c.Database.Password,
	//		},
	//		TLS: nil,
	//		Settings: clickhouse.Settings{
	//			"max_execution_time": 60,
	//		},
	//		DialTimeout:          time.Second * 30,
	//		Debug:                false,
	//		BlockBufferSize:      10,
	//		MaxCompressionBuffer: 10240,
	//		ClientInfo: clickhouse.ClientInfo{
	//			Products: []struct {
	//				Name    string
	//				Version string
	//			}{
	//				{Name: clickhouseClientName, Version: clickhouseClientVersion},
	//			},
	//		},
	//	})
	//conn, err = clickhouse.Open(&clickhouse.Options{
	//	Addr: []string{c.Database.Addr},
	//	Auth: clickhouse.Auth{
	//		Database: c.Database.Database,
	//		Username: c.Database.Username,
	//		Password: c.Database.Password,
	//	},
	//	ClientInfo: clickhouse.ClientInfo{
	//		Products: []struct {
	//			Name    string
	//			Version string
	//		}{
	//			{Name: clickhouseClientName, Version: clickhouseClientVersion},
	//		},
	//	},
	//	Debug: false,
	//	Debugf: func(format string, v ...interface{}) {
	//		fmt.Printf(format, v)
	//	},
	//	TLS: nil,
	//})
	//)

	connStr := fmt.Sprintf("tcp://%s/%s?dial_timeout=1s%s", c.Database.Addr, c.Database.Database, getCreds(*c))
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
		creds = fmt.Sprintf("&username=%s&password=%s", cfg.Database.Username, cfg.Database.Password)
	}

	return creds
}
