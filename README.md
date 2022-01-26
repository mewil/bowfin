# Bowfin

[![Docker Hub](https://img.shields.io/docker/pulls/mewil/bowfin.svg)](https://hub.docker.com/repository/docker/mewil/bowfin)

Exports [Elasticsearch](https://elastic.co/) documents to an [S3](https://aws.amazon.com/s3/) compatible API in [NDJSON](http://ndjson.org/) format.

## Docker Quickstart

With Elasticsearch running on your local machine on port `9200`, use the following `docker run` command with appropriate 
credentials to export all documents in the `my-data` index to the `my-backups` bucket with the key `/my-data.ndjson`.
```shell script
docker run --rm \                                                                                                                                                  athena at macbook-pro (-)(main)
    -e BOWFIN_ES_HOST="http://localhost:9200" \
    -e BOWFIN_ES_INDEX="my-data" \
    -e BOWFIN_S3_BUCKET_NAME="my-backups" \
    -e BOWFIN_S3_ACCESS_KEY_ID="<S3_ACCESS_KEY_ID>" \
    -e BOWFIN_S3_SECRET_ACCESS_KEY="<S3_SECRET_ACCESS_KEY>" \
    mewil/bowfin:latest
```

## Options

Bowfin can be configured using the following environment variables:

| Variable                      | Description                                                                          | Default      |
| ----------------------------- | ------------------------------------------------------------------------------------ | ------------ |
| `BOWFIN_ES_HOST`              | An Elasticsearch host including URL scheme (e.g. `http://host.docker.internal:9200`) | `""`         |
| `BOWFIN_ES_INDEX`             | The name of the index you wish to export                                             | `""`         | 
| `BOWFIN_S3_BUCKET_NAME`       | The object store bucket where the exported object should be stored                   | `""`         | 
| `BOWFIN_S3_BUCKET_PREFIX`     | A prefix added to the key of the exported object                                     | `""`         | 
| `BOWFIN_S3_ACCESS_KEY_ID`     | Access key used for authentication                                                   | `""`         |
| `BOWFIN_S3_SECRET_ACCESS_KEY` | Secret key used for authentication                                                   | `""`         |
| `BOWFIN_S3_REGION`            | Object store region                                                                  | `"us-east-1` |
| `BOWFIN_S3_ENDPOINT`          | Object store endpoint                                                                | `""`         |
| `BOWFIN_SCROLL_SIZE`          | Elasticsearch scroll size                                                            | `10000`      |
| `BOWFIN_CONCURRENT_UPLOADS`   | Number of object parts to upload concurrently                                        | `10`         | 

_Disclaimer:_ Bowfin is actively under development and may not behave as expected.