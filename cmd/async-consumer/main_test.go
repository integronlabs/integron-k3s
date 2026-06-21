package main

import (
	"encoding/base64"
	"testing"

	"github.com/integronlabs/integron-async/engine"
	kafka "github.com/segmentio/kafka-go"
)

func msg(topic string, partition int, offset int64) kafka.Message {
	return kafka.Message{Topic: topic, Partition: partition, Offset: offset}
}

func failure(topic string, partition int, offset int64) engine.BatchItemFailure {
	return engine.BatchItemFailure{ItemIdentifier: engine.KafkaItemIdentifier{
		Topic:     topic,
		Partition: int64(partition),
		Offset:    offset,
	}}
}

func TestCommittableMessages(t *testing.T) {
	msgs := []kafka.Message{
		msg("t", 0, 10), msg("t", 0, 11), msg("t", 0, 12),
		msg("t", 1, 5), msg("t", 1, 6),
	}

	t.Run("no failures commits everything", func(t *testing.T) {
		got, ok := committableMessages(msgs, engine.BatchResponse{})
		if !ok {
			t.Fatal("ok = false, want true")
		}
		if len(got) != len(msgs) {
			t.Fatalf("committable = %d, want %d", len(got), len(msgs))
		}
	})

	t.Run("commits below the lowest failed offset per partition", func(t *testing.T) {
		// Partition 0 fails at 11 (so 10 is safe, 11 and 12 are held).
		// Partition 1 has no failures (5 and 6 both safe).
		resp := engine.BatchResponse{BatchItemFailures: []engine.BatchItemFailure{
			failure("t", 0, 12),
			failure("t", 0, 11),
		}}
		got, ok := committableMessages(msgs, resp)
		if !ok {
			t.Fatal("ok = false, want true")
		}
		want := map[int64]bool{10: true, 5: true, 6: true}
		if len(got) != len(want) {
			t.Fatalf("committable = %d, want %d", len(got), len(want))
		}
		for _, m := range got {
			if !want[m.Offset] {
				t.Errorf("offset %d should not be committed", m.Offset)
			}
		}
	})

	t.Run("unparseable identifier holds the whole batch", func(t *testing.T) {
		resp := engine.BatchResponse{BatchItemFailures: []engine.BatchItemFailure{
			{ItemIdentifier: map[string]any{"topic": "t"}},
		}}
		got, ok := committableMessages(msgs, resp)
		if ok {
			t.Fatal("ok = true, want false")
		}
		if got != nil {
			t.Fatalf("committable = %v, want nil", got)
		}
	})
}

func TestToRecordsBase64EncodesValueAndKey(t *testing.T) {
	records := toRecords([]kafka.Message{{
		Topic: "t", Partition: 2, Offset: 7,
		Key: []byte("k"), Value: []byte(`{"a":1}`),
	}})
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	r := records[0]
	if r.Topic != "t" || r.Partition != 2 || r.Offset != 7 {
		t.Errorf("coords = %q/%d/%d, want t/2/7", r.Topic, r.Partition, r.Offset)
	}
	if got, _ := base64.StdEncoding.DecodeString(r.Value); string(got) != `{"a":1}` {
		t.Errorf("decoded value = %q, want {\"a\":1}", got)
	}
	if got, _ := base64.StdEncoding.DecodeString(r.Key); string(got) != "k" {
		t.Errorf("decoded key = %q, want k", got)
	}
}

func TestSplitList(t *testing.T) {
	got := splitList(" a, b ,,c ")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("splitList = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("splitList[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
