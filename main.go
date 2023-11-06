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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/urfave/cli/v2"
)

func deleteObjects(svc *s3.Client, bucketName string, keys []string, wg *sync.WaitGroup, counter *atomic.Uint64) {
	defer wg.Done()

	_, err := svc.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: &bucketName,
		Delete: &types.Delete{
			Objects: func() []types.ObjectIdentifier {
				identifiers := make([]types.ObjectIdentifier, len(keys))
				for i := range keys {
					key := keys[i]
					identifiers[i] = types.ObjectIdentifier{
						Key: &key,
					}
				}
				return identifiers
			}(),
		},
	})

	if err != nil {
		slog.Error("failed to delete objects", "keys", keys, "error", err)
		return
	}

	for _, key := range keys {
		slog.Debug("deleted object", "key", key)
	}
	counter.Add(uint64(len(keys)))
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

			const batchSize = 500   // Group objects into batches of 500
			var objectKeys []string // This slice will accumulate keys to delete in a batch

			for paginator.HasMorePages() {
				output, err := paginator.NextPage(context.TODO())
				if err != nil {
					return fmt.Errorf("failed to list objects: %v", err)
				}

				for _, item := range output.Contents {
					objectKeys = append(objectKeys, aws.ToString(item.Key))

					// If we have reached the batchSize, delete these objects as a batch
					if len(objectKeys) == batchSize {
						sem <- struct{}{} // Acquire concurrency slot
						wg.Add(1)
						go func(keysToDelete []string) {
							defer func() {
								<-sem // Release concurrency slot
							}()
							deleteObjects(svc, bucketName, keysToDelete, &wg, &deleteCounter)
						}(objectKeys)
						objectKeys = nil // Reset the slice for the next batch
					}
				}
			}

			// After exiting the loop, check if there are any remaining keys to delete
			if len(objectKeys) > 0 {
				sem <- struct{}{} // Acquire concurrency slot
				wg.Add(1)
				go deleteObjects(svc, bucketName, objectKeys, &wg, &deleteCounter)
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
