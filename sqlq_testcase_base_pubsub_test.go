package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func (tc *TestCase) BasicPubSub(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	// Create channels to track job processing
	jobProcessed := make(chan bool, 1)
	var receivedPayload TestPayload

	// Consume the queue
	tc.Q.Consume(ctx, "test_job", "test_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		var payload TestPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			t.Errorf("Failed to unmarshal payload: %v", err)
			return err
		}

		receivedPayload = payload

		jobProcessed <- true
		return nil
	})

	// Publish a job
	testPayload := TestPayload{Message: "Hello, World!", Count: 42}
	err := tc.Q.Publish(ctx, "test_job", testPayload)
	require.NoError(t, err, "Failed to publish job")

	// Wait for the job to be processed with a longer timeout
	select {
	case <-jobProcessed:
		// Job was processed
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for job to be processed")
	}

	// Verify the payload was correctly received
	require.Equal(t, testPayload.Message, receivedPayload.Message, "Message mismatch")
	require.Equal(t, testPayload.Count, receivedPayload.Count, "Count mismatch")
}

func (tc *TestCase) BasicPubMultiSub(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	// Create channels to track job processing
	consumer1Processed := make(chan bool, 1)
	consumer2Processed := make(chan bool, 1)

	// Consume the first queue
	tc.Q.Consume(ctx, "shared_job", "consumer1", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		consumer1Processed <- true
		return nil
	})

	// Consume the second queue
	tc.Q.Consume(ctx, "shared_job", "consumer2", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		consumer2Processed <- true
		return nil
	})

	// Publish a job
	err := tc.Q.Publish(ctx, "shared_job", TestPayload{Message: "Shared job", Count: 100})
	require.NoError(t, err, "Failed to publish shared job")

	// Wait for both consumers to process the job
	for i, ch := range []chan bool{consumer1Processed, consumer2Processed} {
		consumerName := fmt.Sprintf("consumer%d", i+1)
		select {
		case <-ch:
			// Job was processed by this consumer
		case <-time.After(5 * time.Second):
			t.Fatalf("Timed out waiting for job to be processed by %s", consumerName)
		}
	}
}
