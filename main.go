package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/urfave/cli/v2"
)

func deleteObject(svc *s3.Client, bucketName string, key string, wg *sync.WaitGroup, counter *atomic.Uint64) {
	defer wg.Done()

	_, err := svc.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})

	if err != nil {
		slog.Error("failed to delete object", "key", key, "error", err)
		return
	}

	slog.Debug("deleted object", "key", key)
	counter.Add(1)
}

func main() {
	app := &cli.App{
		Name:  "s3purge",
		Usage: "Delete all files in an S3-compatible bucket",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "endpoint",
				Usage:    "S3-compatible endpoint URL",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "bucket",
				Usage:    "Name of the S3 bucket",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "accessKey",
				Usage:    "Access key ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "secretKey",
				Usage:    "Secret access key",
				Required: true,
			},
			&cli.Int64Flag{
				Name:  "concurrency",
				Usage: "Number of concurrent deletions",
				Value: 250,
			},
			&cli.DurationFlag{
				Name:  "rateDisplayInterval",
				Usage: "Interval to display deletion rate",
				Value: 5 * time.Second,
			},
			&cli.StringFlag{
				Name:  "logLevel",
				Usage: "Log level (debug, info, warn, error)",
				Value: "info",
			},
		},
		Action: func(c *cli.Context) error {
			endpoint := c.String("endpoint")
			bucketName := c.String("bucket")
			accessKeyID := c.String("accessKey")
			secretAccessKey := c.String("secretKey")

			logLvl := new(slog.LevelVar)
			logLvl.UnmarshalText([]byte(c.String("logLevel")))
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level: logLvl,
			})))

			slog.Info("Starting S3 purge", "endpoint", endpoint, "bucket", bucketName, "concurrency", c.Int64("concurrency"))

			cfg, err := config.LoadDefaultConfig(context.TODO(),
				config.WithEndpointResolver(aws.EndpointResolverFunc(
					func(service, region string) (aws.Endpoint, error) {
						return aws.Endpoint{
							URL: endpoint,
						}, nil
					},
				)),
				config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
			)
			if err != nil {
				return fmt.Errorf("unable to load SDK config: %v", err)
			}

			svc := s3.NewFromConfig(cfg)

			// Paginator to list all the objects in the bucket
			paginator := s3.NewListObjectsV2Paginator(svc, &s3.ListObjectsV2Input{
				Bucket: &bucketName,
			})

			var wg sync.WaitGroup
			deleteCounter := atomic.Uint64{}
			startTime := time.Now()

			go func() {
				for {
					time.Sleep(c.Duration("rateDisplayInterval"))
					duration := time.Since(startTime).Seconds()
					rate := float64(deleteCounter.Load()) / duration
					slog.Info(fmt.Sprintf("Current deletion rate: %.3f items/second", rate))
				}
			}()

			sem := make(chan struct{}, c.Int64("concurrency"))

			for paginator.HasMorePages() {
				output, err := paginator.NextPage(context.TODO())
				if err != nil {
					return fmt.Errorf("failed to list objects: %v", err)
				}

				for _, item := range output.Contents {
					sem <- struct{}{} // Acquire concurrency slot
					wg.Add(1)
					go func(key string) {
						defer func() {
							<-sem // Release concurrency slot
						}()
						deleteObject(svc, bucketName, key, &wg, &deleteCounter)
					}(aws.ToString(item.Key))
				}
			}

			wg.Wait() // Wait for all deletions to complete
			slog.Info(fmt.Sprintf("Deleted %d objects", deleteCounter.Load()))
			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
