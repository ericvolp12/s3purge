# S3 Purge

`s3purge` is a simple Go program to purge the contents of an S3 bucket in parallel.

Existing methods of purging S3 buckets (i.e. `rclone purge`) are rather slow and don't have the performance required to expeditiously delete large numbers (hundreds of thousands to millions) of objects in a reasonable timeframe.

`s3purge` has been tested on Cloudflare's R2 object storage to delete ~1,000 objects per second using the default concurrency of `250`.

## Building and Running

Build with the included makefile using:

```shell
make build
```

Run the binary produced with flags for your S3 backend:

```shell
$ ./s3purge --endpoint {your_s3_backend_https_url} --accessKey {your_key} --secretKey {your_secret} --bucket {your_bucket_name}
```


By default, `s3purge` outputs a progress marker every 5 seconds with the average rate of deletion.

When no objects remain, `s3purge` will exit and tell you how many objects it deleted.
