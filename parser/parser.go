package parser

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nugored/cf-logs-loki-uploader/loki"
	"github.com/nugored/cf-logs-loki-uploader/models"
)

type Parser struct {
	opts     models.Options
	s3Client *s3.Client
	logger   *slog.Logger
	queue    chan *string
	stop     bool
}

func parseDataLine(line string, headerFields []string) (models.LogEntry, error) {
	fields := strings.Fields(line) // Simple space split

	if len(fields) != len(headerFields) {
		return nil, fmt.Errorf("field count mismatch: expected %d, got %d", len(headerFields), len(fields))
	}

	entry := make(models.LogEntry)
	for i, name := range headerFields {
		entry[name] = fields[i]
	}
	return entry, nil
}

func NewParser(opts models.Options, s3Client *s3.Client, logger *slog.Logger) *Parser {
	parser := &Parser{
		opts:     opts,
		s3Client: s3Client,
		logger:   logger,
		queue:    make(chan *string, 10*opts.Workers),
	}
	return parser
}

// Stop gracefully all workers
func (s *Parser) Stop() {
	if s.stop {
		return
	}
	s.stop = true
	close(s.queue)
}

func (s *Parser) Scan() error {
	num := 0
	ctx := context.Background()
	maxKeys := int32(100) //no pager, tune interval to have less files per run
	output, err := s.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &s.opts.BucketName,
		MaxKeys: &maxKeys,
	})
	if err != nil {
		return err
	}

	start := time.Now()
	for _, obj := range output.Contents {
		if obj.Key == nil || s.stop {
			continue
		}
		s.queue <- obj.Key
		num++
	}
	if num > 0 {
		s.logger.Info("new files", "found", num, "duration", time.Since(start), "queue", len(s.queue))
	}
	return nil
}

func (s *Parser) Worker() error {
	ctx := context.Background() // limit time to process file? will restart of processing help?

	for fn := range s.queue {

		if err := s.parseFile(ctx, *fn); err != nil {
			s.logger.Error("failed to ship file", "key", *fn, "err", err)
			return err // pod restart instead of deletion of not-shipped file
		}

		if _, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &s.opts.BucketName,
			Key:    fn,
		}); err != nil {
			s.logger.Error("failed to delete file", "key", *fn, "err", err)
		}

	}
	return nil
}

func (s *Parser) parseFile(ctx context.Context, fn string) error {
	start := time.Now()

	parts := strings.Split(fn, "/")
	namespace := parts[0]
	cloudfrontObjectName := parts[1]

	labels := map[string]string{
		"namespace":  namespace,
		"cloudfront": cloudfrontObjectName,
	}

	labels["cluster"] = s.opts.ClusterName
	labels["index"] = fmt.Sprintf("%s-%s", s.opts.ClusterName, namespace)

	for k, v := range s.opts.Labels {
		labels[k] = v
	}

	b := loki.NewBatch(labels, s.opts, s.logger)

	obj, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.opts.BucketName,
		Key:    &fn,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			s.logger.Debug("skipping non-existent file", "key", fn)
			return nil
		}
		return fmt.Errorf("failed to get object %s: %w", fn, err)
	}
	defer obj.Body.Close()

	gzreader, err := gzip.NewReader(obj.Body)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzreader.Close()

	var lineCount int

	scanner := bufio.NewScanner(gzreader)
	w3cLog := models.W3CLog{}

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "#Fields:") {
			// Found the header line
			parts := strings.Fields(line)
			// The header starts after "#Fields:"
			w3cLog.HeaderFields = parts[1:]
			continue
		}

		if strings.HasPrefix(line, "#") || len(w3cLog.HeaderFields) == 0 {
			// Skip other directives or lines before the header is found
			continue
		}

		// This is a data line, use the custom parser
		entry, err := parseDataLine(line, w3cLog.HeaderFields)
		if err != nil {
			return fmt.Errorf("error parsing data line: %w", err)
		}

		jsonData, err := json.Marshal(entry)
		if err != nil {
			log.Fatalf("Error marshaling map to JSON: %v", err)
		}

		// Convert the byte slice to a human-readable string and print
		jsonString := string(jsonData)
		// fmt.Println("JSON String (Compact):")
		// fmt.Println(jsonString)

		ts := time.Now()
		if err = b.Add(ts, jsonString); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}

	}

	if err := scanner.Err(); err != nil {
		return err
	}

	fmt.Printf("Parsed %s\n", fn)

	if err = b.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}
	s.logger.Debug("shipped file", "key", fn, "labels", fmt.Sprintf("%v", labels), "lines", lineCount, "duration", time.Since(start), "lines/s", fmt.Sprintf("%.2f", float64(lineCount)/time.Since(start).Seconds()))
	return nil

}

func (s *Parser) Metrics() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "cloudfront_logs_shipper_queue_length %d\n", len(s.queue))
	})
}
