package sqlq_test

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func newTracer(ctx context.Context, otlpEndpoint string) (tracer trace.Tracer, cleanup func() error, err error) {
	var exporter sdktrace.SpanExporter

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if otlpEndpoint == "" {
		exporter, err = stdouttrace.New()
	} else {
		exporter, err = otlptracehttp.New(
			ctx,
			otlptracehttp.WithInsecure(),
			otlptracehttp.WithEndpoint(otlpEndpoint),
		)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("create exporter: %w", err)
	}

	resource, err := getOtelResource()
	if err != nil {
		return nil, nil, err
	}

	traceProvider := sdktrace.NewTracerProvider(
		//sdktrace.WithBatcher(exporter), //recommended over Syncer for production, but our volume is low enough
		sdktrace.WithSyncer(exporter),
		sdktrace.WithResource(resource),
	)
	cleanup = func() error {
		return traceProvider.Shutdown(ctx)
	}

	otel.SetTracerProvider(traceProvider)

	tracer = traceProvider.Tracer("sqlq_test")

	return tracer, cleanup, nil
}
func getOtelResource() (*sdkresource.Resource, error) {
	return sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
			semconv.SchemaURL,
		),
	)
}
