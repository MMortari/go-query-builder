package query

type Config struct {
	parseWhere bool
}
type QueryBuilderConfig func(*QueryBuilder)

func ParseWhere(parse bool) QueryBuilderConfig {
	return func(q *QueryBuilder) {
		q.config.parseWhere = parse
	}
}
