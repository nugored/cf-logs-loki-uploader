package models

import "time"

type Options struct {
	BucketName   string
	WaitInterval time.Duration
	Format       string
	LokiURL      string
	LokiUser     string
	LokiPassword string
	ClusterName  string
	Labels       map[string]string
	Workers      int
	Port         int
}
