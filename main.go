package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/kelseyhightower/envconfig"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/semaphore"
	"io"
	"log"
	"sync"
	"time"
)

type appConfig struct {
	EsHost            string        `split_words:"true"`
	EsIndex           string        `split_words:"true"`
	EsScrollTime      time.Duration `split_words:"true" default:"30m"`
	S3BucketName      string        `split_words:"true"`
	S3BucketPrefix    string        `split_words:"true"`
	S3AccessKeyID     string        `split_words:"true"`
	S3SecretAccessKey string        `split_words:"true"`
	S3Region          string        `split_words:"true" default:"us-east-1"`
	S3Endpoint        string        `split_words:"true"`
	ScrollSize        int           `split_words:"true" default:"10000"`
	ConcurrentUploads int           `split_words:"true" default:"10"`
}

var Config appConfig

const (
	fiveMBInBytes = 5 * 1024 * 1024
)

func main() {
	if err := envconfig.Process("bowfin", &Config); err != nil {
		log.Fatal(err)
	}

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{Config.EsHost},
	})
	if err != nil {
		log.Fatal(err)
	}
	if _, err = es.Ping(); err != nil {
		log.Fatal("failed to connect to elasticsearch: ", err)
	}

	s3Client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Region:   aws.String(Config.S3Region),
			Endpoint: aws.String(Config.S3Endpoint),
			Credentials: credentials.NewStaticCredentials(
				Config.S3AccessKeyID,
				Config.S3SecretAccessKey,
				"",
			),
		},
	)))
	key := Config.S3BucketPrefix + Config.EsIndex + ".ndjson"
	multipartUpload, err := s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(Config.S3BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("starting upload", *multipartUpload.UploadId, "to key", key)
	documents := make(chan []byte, Config.ScrollSize)
	done := make(chan struct{})

	go upload(documents, done, s3Client, multipartUpload)

	docCount := 0
	if err = scroll(es, func(key, value gjson.Result) bool {
		documents <- []byte(value.String())
		docCount++
		return true
	}); err != nil {
		log.Fatal(err)
	}
	close(documents)
	<-done
	log.Println("successfully uploaded", docCount, "documents to key", key)
}

func upload(documents <-chan []byte, done chan<- struct{}, s3Client *s3.S3, multipartUpload *s3.CreateMultipartUploadOutput) {
	uploadedParts := make(chan *s3.CompletedPart)
	completedParts := make([]*s3.CompletedPart, 0)
	s := semaphore.NewWeighted(int64(Config.ConcurrentUploads))
	wg := &sync.WaitGroup{}
	go func() {
		for t := range uploadedParts {
			completedParts = append(completedParts, t)
			s.Release(1)
			wg.Done()
		}
	}()
	for part := 1; ; part++ {
		batch, ok := batchDocuments(documents)
		if !ok && len(batch) == 0 {
			break
		}
		_ = s.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(b []byte, p int) {
			eTag, err := uploadPart(s3Client, multipartUpload, b, p)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("uploaded part %d (%d bytes)", p, len(b))
			uploadedParts <- &s3.CompletedPart{
				ETag:       eTag,
				PartNumber: aws.Int64(int64(p)),
			}
		}(batch, part)
	}
	wg.Wait()
	close(uploadedParts)
	if _, err := s3Client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		UploadId: multipartUpload.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}); err != nil {
		log.Fatal("failed to complete upload: ", err)
	}
	done <- struct{}{}
}

func batchDocuments(documents <-chan []byte) ([]byte, bool) {
	buf := bytes.NewBuffer(nil)
	for doc := range documents {
		if _, err := buf.Write(doc); err != nil {
			log.Fatal("failed to write document to buffer:", err)
		}
		if _, err := buf.WriteRune('\n'); err != nil {
			log.Fatal("failed to write newline to buffer:", err)
		}
		if buf.Len() >= fiveMBInBytes {
			return buf.Bytes(), true
		}
	}
	return buf.Bytes(), false
}

func uploadPart(s3Client *s3.S3, upload *s3.CreateMultipartUploadOutput, data []byte, part int) (*string, error) {
	res, err := s3Client.UploadPart(&s3.UploadPartInput{
		Body:          bytes.NewReader(data),
		Bucket:        upload.Bucket,
		Key:           upload.Key,
		PartNumber:    aws.Int64(int64(part)),
		UploadId:      upload.UploadId,
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		if _, abortErr := s3Client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   upload.Bucket,
			Key:      upload.Key,
			UploadId: upload.UploadId,
		}); abortErr != nil {
			return nil, fmt.Errorf("failed to abort multipart upload while handing err '%v': %w", err, abortErr)
		}
		return nil, fmt.Errorf("failed to upload part %d: %w", part, err)
	}
	return res.ETag, nil
}

func scroll(es *elasticsearch.Client, hitCallback func(key, value gjson.Result) bool) error {
	data, err := doEsRequest(es.Search(
		es.Search.WithIndex(Config.EsIndex),
		es.Search.WithSize(Config.ScrollSize),
		es.Search.WithScroll(Config.EsScrollTime),
	))
	if err != nil {
		return err
	}
	hits := gjson.GetBytes(data, "hits.hits")
	hits.ForEach(hitCallback)
	scrollID := gjson.GetBytes(data, "_scroll_id").String()
	for {
		data, err = doEsRequest(es.Scroll(
			es.Scroll.WithScroll(Config.EsScrollTime),
			es.Scroll.WithScrollID(scrollID),
		))
		if err != nil {
			return err
		}
		hits = gjson.GetBytes(data, "hits.hits")
		if len(hits.Array()) == 0 {
			log.Println("no more document hits")
			break
		}
		hits.ForEach(hitCallback)
		scrollID = gjson.GetBytes(data, "_scroll_id").String()
	}
	return nil
}

func doEsRequest(res *esapi.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf("got error response: %s", res)
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}
