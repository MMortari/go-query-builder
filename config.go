package query

import (
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	parseWhere bool
}
type QueryBuilderConfig func(*QueryBuilder)

func ParseWhere(parse bool) QueryBuilderConfig {
	return func(q *QueryBuilder) {
		q.config.parseWhere = parse
	}
}
func SetOtelSpan(span trace.Span) QueryBuilderConfig {
	return func(q *QueryBuilder) {
		q.otelSpan = span
	}
}
