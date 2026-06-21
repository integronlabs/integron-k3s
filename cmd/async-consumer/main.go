// Command async-consumer is the runtime for an IntegronAsyncAPI: a long-lived
// Kafka consumer that runs integron-async workflows per message.
//
// It adapts github.com/integronlabs/integron-async — which natively consumes
// Kafka only indirectly, through AWS EventBridge Pipes into Lambda — to a plain
// Kubernetes Deployment. It parses the AsyncAPI document into the same engine
// the Lambda path uses, then drives that engine directly from a native Kafka
// consumer group: poll a batch, map to engine.KafkaRecord, ProcessBatch, and
// commit only the offsets the engine did not report as failures (at-least-once;
// failed offsets are left uncommitted so they are redelivered).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/integronlabs/integron-async/asyncapi"
	"github.com/integronlabs/integron-async/engine"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// config is the consumer's runtime configuration, sourced from flags and env.
type config struct {
	specPath  string
	brokers   []string
	groupID   string
	topics    []string // explicit override; empty means "use the spec's topics"
	minBytes  int
	maxBytes  int
	batchSize int
	maxWait   time.Duration

	tlsEnabled    bool
	tlsSkipVerify bool
	tlsCAFile     string

	saslMechanism string
	saslUsername  string
	saslPassword  string
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	eng, topics, err := buildEngine(cfg)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}
	log.Printf("subscribing to topics %v (group %q) on %v", topics, cfg.groupID, cfg.brokers)

	dialer, err := buildDialer(cfg)
	if err != nil {
		log.Fatalf("kafka dialer: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.brokers,
		GroupID:     cfg.groupID,
		GroupTopics: topics,
		Dialer:      dialer,
		MinBytes:    cfg.minBytes,
		MaxBytes:    cfg.maxBytes,
		// Manual, synchronous commits: we commit only the offsets the engine
		// accepted, so a non-zero CommitInterval would defeat selective retry.
		CommitInterval: 0,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, reader, eng, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("consumer: %v", err)
	}
	log.Printf("shutting down")
}

// run is the consume loop: collect a batch, process it, commit accepted offsets.
func run(ctx context.Context, reader *kafka.Reader, eng *engine.Engine, cfg config) error {
	for {
		msgs, err := collectBatch(ctx, reader, cfg.batchSize, cfg.maxWait)
		if len(msgs) == 0 {
			if err != nil {
				return err
			}
			continue
		}

		resp := eng.ProcessBatch(ctx, toRecords(msgs))
		committable, ok := committableMessages(msgs, resp)
		if !ok {
			log.Printf("could not interpret batch failures; leaving %d messages uncommitted for retry", len(msgs))
		}
		if len(committable) > 0 {
			// Use a fresh context so a shutdown signal does not abort the commit
			// of work already done.
			cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if cerr := reader.CommitMessages(cctx, committable...); cerr != nil {
				cancel()
				return fmt.Errorf("committing offsets: %w", cerr)
			}
			cancel()
		}
		if n := len(resp.BatchItemFailures); n > 0 {
			log.Printf("processed %d messages, %d committed, %d failed (will be redelivered)", len(msgs), len(committable), n)
		}

		if err != nil {
			return err // surfaced after committing what we processed
		}
	}
}

// collectBatch blocks for the first message, then gathers up to batchSize
// messages, returning early once maxWait elapses since the first one. A returned
// error (context cancellation) is reported alongside any messages already read
// so they can still be processed before shutting down.
func collectBatch(ctx context.Context, reader *kafka.Reader, batchSize int, maxWait time.Duration) ([]kafka.Message, error) {
	first, err := reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	msgs := []kafka.Message{first}

	deadline := time.Now().Add(maxWait)
	for len(msgs) < batchSize {
		wctx, cancel := context.WithDeadline(ctx, deadline)
		m, ferr := reader.FetchMessage(wctx)
		cancel()
		if ferr != nil {
			if ctx.Err() != nil {
				return msgs, ctx.Err() // shutdown: keep what we have
			}
			// Deadline for this batch window elapsed; process what we have.
			break
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// toRecords maps Kafka messages to the engine's record type. The engine expects
// Value and Key base64-encoded (matching the EventBridge Pipes wire format) and
// decodes them itself.
func toRecords(msgs []kafka.Message) []engine.KafkaRecord {
	records := make([]engine.KafkaRecord, len(msgs))
	for i, m := range msgs {
		records[i] = engine.KafkaRecord{
			Topic:     m.Topic,
			Partition: int64(m.Partition),
			Offset:    m.Offset,
			Timestamp: m.Time.UnixMilli(),
			Value:     base64.StdEncoding.EncodeToString(m.Value),
			Key:       base64.StdEncoding.EncodeToString(m.Key),
		}
	}
	return records
}

type partitionKey struct {
	topic     string
	partition int
}

// committableMessages returns the messages safe to commit: every message whose
// offset is below the lowest failed offset for its partition (committing an
// offset implies all lower offsets in that partition are done). The bool is
// false if a failure identifier could not be interpreted, in which case the
// caller should commit nothing and let the whole batch be redelivered.
func committableMessages(msgs []kafka.Message, resp engine.BatchResponse) ([]kafka.Message, bool) {
	minFailed := map[partitionKey]int64{}
	for _, f := range resp.BatchItemFailures {
		id, ok := f.ItemIdentifier.(engine.KafkaItemIdentifier)
		if !ok {
			return nil, false
		}
		k := partitionKey{id.Topic, int(id.Partition)}
		if cur, seen := minFailed[k]; !seen || id.Offset < cur {
			minFailed[k] = id.Offset
		}
	}

	var committable []kafka.Message
	for _, m := range msgs {
		if floor, ok := minFailed[partitionKey{m.Topic, m.Partition}]; ok && m.Offset >= floor {
			continue
		}
		committable = append(committable, m)
	}
	return committable, true
}

// buildEngine loads and parses the AsyncAPI spec, builds the workflow engine and
// resolves the topics to subscribe to (config override, else the spec's topics).
func buildEngine(cfg config) (*engine.Engine, []string, error) {
	specData, err := os.ReadFile(cfg.specPath)
	if err != nil {
		return nil, nil, fmt.Errorf("reading spec %q: %w", cfg.specPath, err)
	}
	doc, err := asyncapi.Parse(specData)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing AsyncAPI document: %w", err)
	}
	topicMap, err := doc.GetTopicToOperationMap()
	if err != nil {
		return nil, nil, fmt.Errorf("resolving AsyncAPI topics: %w", err)
	}
	if len(topicMap) == 0 {
		return nil, nil, errors.New("AsyncAPI document declares no topics")
	}

	topics := cfg.topics
	if len(topics) == 0 {
		for topic := range topicMap {
			topics = append(topics, topic)
		}
	}
	return engine.NewEngine(topicMap), topics, nil
}

// buildDialer constructs a Kafka dialer with optional TLS and SASL.
func buildDialer(cfg config) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true}

	if cfg.tlsEnabled {
		tlsCfg := &tls.Config{InsecureSkipVerify: cfg.tlsSkipVerify} //nolint:gosec // opt-in via spec
		if cfg.tlsCAFile != "" {
			pem, err := os.ReadFile(cfg.tlsCAFile)
			if err != nil {
				return nil, fmt.Errorf("reading TLS CA %q: %w", cfg.tlsCAFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("no certificates found in TLS CA %q", cfg.tlsCAFile)
			}
			tlsCfg.RootCAs = pool
		}
		dialer.TLS = tlsCfg
	}

	if cfg.saslMechanism != "" {
		mech, err := saslMechanism(cfg)
		if err != nil {
			return nil, err
		}
		dialer.SASLMechanism = mech
	}
	return dialer, nil
}

func saslMechanism(cfg config) (sasl.Mechanism, error) {
	switch strings.ToUpper(cfg.saslMechanism) {
	case "PLAIN":
		return plain.Mechanism{Username: cfg.saslUsername, Password: cfg.saslPassword}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.saslUsername, cfg.saslPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.saslUsername, cfg.saslPassword)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", cfg.saslMechanism)
	}
}

// loadConfig reads flags then env, with env taking precedence (the operator sets
// env; flags are for local runs).
func loadConfig() (config, error) {
	cfg := config{
		specPath:  "docs/asyncapi.yaml",
		minBytes:  1,
		maxBytes:  1 << 20,
		batchSize: 100,
		maxWait:   time.Second,
	}
	flag.StringVar(&cfg.specPath, "spec", cfg.specPath, "path to the AsyncAPI document")
	flag.Parse()

	if v := os.Getenv("ASYNCAPI_SPEC_PATH"); v != "" {
		cfg.specPath = v
	}
	cfg.brokers = splitList(os.Getenv("KAFKA_BROKERS"))
	if len(cfg.brokers) == 0 {
		return cfg, errors.New("KAFKA_BROKERS is required")
	}
	cfg.groupID = os.Getenv("KAFKA_GROUP_ID")
	if cfg.groupID == "" {
		return cfg, errors.New("KAFKA_GROUP_ID is required")
	}
	cfg.topics = splitList(os.Getenv("KAFKA_TOPICS"))

	if v, ok, err := envInt("KAFKA_MIN_BYTES"); err != nil {
		return cfg, err
	} else if ok {
		cfg.minBytes = v
	}
	if v, ok, err := envInt("KAFKA_MAX_BYTES"); err != nil {
		return cfg, err
	} else if ok {
		cfg.maxBytes = v
	}
	if v, ok, err := envInt("KAFKA_BATCH_SIZE"); err != nil {
		return cfg, err
	} else if ok && v > 0 {
		cfg.batchSize = v
	}
	if v, ok, err := envInt("KAFKA_MAX_WAIT_MS"); err != nil {
		return cfg, err
	} else if ok && v > 0 {
		cfg.maxWait = time.Duration(v) * time.Millisecond
	}

	cfg.tlsEnabled = os.Getenv("KAFKA_TLS_ENABLED") == "true"
	cfg.tlsSkipVerify = os.Getenv("KAFKA_TLS_INSECURE_SKIP_VERIFY") == "true"
	cfg.tlsCAFile = os.Getenv("KAFKA_TLS_CA_FILE")

	cfg.saslMechanism = os.Getenv("KAFKA_SASL_MECHANISM")
	cfg.saslUsername = os.Getenv("KAFKA_SASL_USERNAME")
	cfg.saslPassword = os.Getenv("KAFKA_SASL_PASSWORD")

	return cfg, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(name string) (int, bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false, fmt.Errorf("%s: %w", name, err)
	}
	return n, true, nil
}
