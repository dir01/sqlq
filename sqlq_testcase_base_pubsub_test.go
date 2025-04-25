package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func (tc *TestCase) TestBasicPubSub(ctx context.Context, t *testing.T) {
	t.Helper()

	// Create channels to track job processing
	jobProcessed := make(chan bool, 1)
	var receivedPayload TestPayload

	// Consume the queue
	err := tc.Q.Consume(ctx, "test_job", "test_consumer", func(_ context.Context, _ *sql.Tx, payloadBytes []byte) error { // Rename unused ctx to _
		var payload TestPayload

		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			t.Errorf("Failed to unmarshal payload: %v", err)
			return err
		}

		receivedPayload = payload
		jobProcessed <- true

		return nil
	})
	require.NoError(t, err, "Failed to start consumer for test_job")

	testPayload := TestPayload{Message: "Hello, World!"}

	err = tc.Q.Publish(ctx, "test_job", testPayload)
	require.NoError(t, err, "Failed to publish job")

	// Wait for the job to be processed with a longer timeout
	select {
	case <-jobProcessed:
		// Job was processed
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for job to be processed")
	}

	require.Equal(t, testPayload.Message, receivedPayload.Message, "Message mismatch")
}

func (tc *TestCase) TestBasicPubMultiSub(ctx context.Context, t *testing.T) {
	t.Helper()

	// Create channels to track job processing
	consumer1Processed := make(chan bool, 1)
	consumer2Processed := make(chan bool, 1)

	// Consume the first queue
	err := tc.Q.Consume(ctx, "shared_job", "consumer1", func(_ context.Context, _ *sql.Tx, _ []byte) error {
		consumer1Processed <- true
		return nil
	})
	require.NoError(t, err)

	// Consume the second queue
	err = tc.Q.Consume(ctx, "shared_job", "consumer2", func(_ context.Context, _ *sql.Tx, _ []byte) error {
		consumer2Processed <- true
		return nil
	})
	require.ErrorIs(t, err, sqlq.ErrDuplicateConsumer)
}
