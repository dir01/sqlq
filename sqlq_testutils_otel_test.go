package sqlq_test

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func newTracer(ctx context.Context) (trace.Tracer, func() error, error) {
	var exporter sdktrace.SpanExporter
	var err error

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	otlpEndpoint := os.Getenv("OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		return noop.NewTracerProvider().Tracer("sqlq_test"), func() error { return nil }, nil
	}

	exporter, err = otlptracehttp.New(
		ctx,
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithEndpoint(otlpEndpoint),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("create exporter: %w", err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		// sdktrace.WithBatcher(exporter), // recommended over Syncer for production, but our volume is low enough // Add space after //
		sdktrace.WithSyncer(exporter),
		sdktrace.WithResource(sdkresource.Default()),
	)
	cleanup := func() error {
		return traceProvider.Shutdown(ctx)
	}

	otel.SetTracerProvider(traceProvider)

	tracer := traceProvider.Tracer("sqlq_test")

	return tracer, cleanup, nil
}
