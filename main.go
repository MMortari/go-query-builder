package query

import (
	"fmt"
	"reflect"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Where struct {
	Column string
	Type   string
	Val    interface{}
}
type Value struct {
	Column string
	Val    interface{}
}
type OrderBy struct {
	Column string
	Type   string
}

type JoinType string

const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
	FullJoin  JoinType = "FULL JOIN"
)

type Join struct {
	Table string
	As    string
	On    string
	Type  JoinType
}

type QueryBuilder struct {
	config Config

	otelSpan trace.Span

	from      string
	selects   []string
	values    []Value
	joins     []Join
	wheresAnd [][]Where
	wheresOr  [][]Where
	limit     *int
	offset    *int
	groupBy   []string
	orderBys  []OrderBy
}

func NewQueryBuilder(configs ...QueryBuilderConfig) *QueryBuilder {
	config := Config{
		parseWhere: true,
	}

	qb := &QueryBuilder{
		config: config,
	}

	for _, item := range configs {
		item(qb)
	}

	return qb
}

func (q *QueryBuilder) From(from ...string) *QueryBuilder {
	if len(from) == 1 {
		q.from = fmt.Sprintf(`"%s"`, from[0])
	}
	if len(from) == 2 {
		q.from = fmt.Sprintf(`"%s" AS "%s"`, from[0], from[1])
	}
	q.setSpanAttribute("db.collection.name", from[0])
	return q
}
func (q *QueryBuilder) Select(selects ...string) *QueryBuilder {
	q.selects = selects
	q.setSpanAttribute("db.operation.name", "SELECT")
	return q
}
func (q *QueryBuilder) Values(values ...Value) *QueryBuilder {
	q.values = append(q.values, values...)
	return q
}
func (q *QueryBuilder) ClearSelect() *QueryBuilder {
	q.selects = make([]string, 0)
	return q
}
func (q *QueryBuilder) Join(join Join) *QueryBuilder {
	q.joins = append(q.joins, join)
	return q
}
func (q *QueryBuilder) WhereAnd(where ...Where) *QueryBuilder {
	q.wheresAnd = append(q.wheresAnd, where)
	return q
}
func (q *QueryBuilder) WhereOr(where ...Where) *QueryBuilder {
	q.wheresOr = append(q.wheresOr, where)
	return q
}
func (q *QueryBuilder) PaginationPaged(page int, pageSize int) *QueryBuilder {
	q.limit = &pageSize
	offset := (page - 1) * pageSize
	q.offset = &offset
	return q
}
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.limit = &limit
	return q
}
func (q *QueryBuilder) Offset(offset int) *QueryBuilder {
	q.offset = &offset
	return q
}
func (q *QueryBuilder) OrderBy(orderBy OrderBy) *QueryBuilder {
	q.orderBys = append(q.orderBys, orderBy)
	return q
}
func (q *QueryBuilder) ClearOrderBy() *QueryBuilder {
	q.orderBys = make([]OrderBy, 0)
	return q
}
func (q *QueryBuilder) GroupBy(groupBy ...string) *QueryBuilder {
	q.groupBy = groupBy
	return q
}

func (q *QueryBuilder) ToSelectSql() (query string, queryData []interface{}) {
	qb := strings.Builder{}

	// SELECT
	qb.WriteString("SELECT ")
	if len(q.selects) == 0 {
		qb.WriteString("*")
	} else {
		qb.WriteString(strings.Join(q.selects, ", "))
	}
	qb.WriteString(" ")

	// FROM
	qb.WriteString(fmt.Sprintf(`FROM %s`, q.from))

	// JOIN
	if len(q.joins) != 0 {
		for _, item := range q.joins {
			joinType := InnerJoin

			if item.Type != "" {
				joinType = item.Type
			}

			qb.WriteString(fmt.Sprintf(` %s "%s" AS "%s" ON %s`, joinType, item.Table, item.As, item.On))
		}
	}

	// WHERE
	var where string
	where, queryData = q.getWhere(0)
	qb.WriteString(where)

	// GROUP BY
	if len(q.groupBy) != 0 {
		qb.WriteString(" GROUP BY ")
		qb.WriteString(strings.Join(q.groupBy, ", "))
	}

	// ORDER BY
	if len(q.orderBys) != 0 {
		qb.WriteString(" ORDER BY ")
		orderBy := make([]string, 0, len(q.orderBys))

		for _, item := range q.orderBys {
			if item.Type == "" {
				orderBy = append(orderBy, item.Column)
			} else {
				orderBy = append(orderBy, fmt.Sprintf("%s %s", item.Column, strings.ToUpper(item.Type)))
			}
		}

		qb.WriteString(strings.Join(orderBy, ", "))
	}

	// LIMIT
	if q.limit != nil {
		qb.WriteString(fmt.Sprintf(" LIMIT %d", *q.limit))
	}

	// OFFSET
	if q.offset != nil {
		qb.WriteString(fmt.Sprintf(" OFFSET %d", *q.offset))
	}

	query = qb.String()

	q.setSpanAttribute("db.query.text", query)

	return query, queryData
}
func (q *QueryBuilder) ToSelectTotalSql() (query string, queryData []interface{}) {
	qb := strings.Builder{}

	// SELECT
	qb.WriteString("SELECT COUNT(*) AS total")
	qb.WriteString(" ")

	// FROM
	qb.WriteString(fmt.Sprintf(`FROM %s`, q.from))

	// JOIN
	if len(q.joins) != 0 {
		for _, item := range q.joins {
			joinType := InnerJoin

			if item.Type != "" {
				joinType = item.Type
			}

			qb.WriteString(fmt.Sprintf(` %s "%s" AS "%s" ON %s`, joinType, item.Table, item.As, item.On))
		}
	}

	// WHERE
	var where string
	where, queryData = q.getWhere(0)
	qb.WriteString(where)

	query = qb.String()
	qb = strings.Builder{}

	return query, queryData
}

func (q *QueryBuilder) ToUpdateQuery() (query string, queryData []interface{}) {
	qb := strings.Builder{}

	qb.WriteString("UPDATE ")

	// FROM
	qb.WriteString(fmt.Sprintf(`%s SET `, q.from))

	// VALUES
	var itemNum int

	values := make([]string, 0, len(q.values))
	for _, item := range q.values {
		itemNum++
		values = append(values, fmt.Sprintf(`%s = $%d`, item.Column, itemNum))
		queryData = append(queryData, item.Val)
	}
	qb.WriteString(strings.Join(values, ", "))

	// WHERE
	var where string
	where, queryDataWhere := q.getWhere(itemNum)
	queryData = append(queryData, queryDataWhere...)
	qb.WriteString(where)

	query = qb.String()

	q.setSpanAttribute("db.operation.text", query)

	return query, queryData
}

func (q *QueryBuilder) getWhere(itemNum int) (string, []interface{}) {
	queryData := make([]interface{}, 0)

	if len(q.wheresOr) == 0 && len(q.wheresAnd) == 0 {
		return "", queryData
	}

	qb := strings.Builder{}

	qb.WriteString(" WHERE ")

	wheresToOr := make([]string, 0)

	if len(q.wheresAnd) != 0 {
		whereAndBuilder := make([]string, 0)

		for _, whereAnd := range q.wheresAnd {
			wheres := q.parseWhere(whereAnd, &itemNum, &queryData)
			whereAndBuilder = append(whereAndBuilder, fmt.Sprintf("(%s)", strings.Join(wheres, " AND ")))
		}

		wheresToOr = append(wheresToOr, strings.Join(whereAndBuilder, " AND "))
	}
	if len(q.wheresOr) != 0 {
		for _, whereAnd := range q.wheresOr {
			wheres := q.parseWhere(whereAnd, &itemNum, &queryData)
			wheresToOr = append(wheresToOr, fmt.Sprintf("(%s)", strings.Join(wheres, " AND ")))
		}
	}

	qb.WriteString(strings.Join(wheresToOr, " OR "))

	return qb.String(), queryData
}

func (q *QueryBuilder) parseWhere(whereAnd []Where, itemNum *int, queryData *[]interface{}) []string {
	wheres := make([]string, 0, len(q.wheresAnd))

	for _, item := range whereAnd {
		Type := strings.ToUpper(item.Type)

		var val string

		if item.Val != nil {
			if reflect.TypeOf(item.Val).Kind() == reflect.Slice {
				values := make([]string, 0)
				s := reflect.ValueOf(item.Val)
				for i := 0; i < s.Len(); i++ {
					value := s.Index(i).Interface()

					if q.config.parseWhere {
						(*itemNum)++
						*queryData = append(*queryData, value)
						values = append(values, fmt.Sprintf("$%d", *itemNum))
					} else {
						values = append(values, q.getWhereValue(value))
					}
				}

				if Type == "IN" || Type == "NOT IN" {
					val = fmt.Sprintf("(%s)", strings.Join(values, ", "))
				} else {
					val = strings.Join(values, " AND ")
				}
			} else {
				q.setSpanAttribute("db.query.parameter."+item.Column, fmt.Sprint(item.Val))
				if q.config.parseWhere {
					(*itemNum)++
					*queryData = append(*queryData, item.Val)
					val = fmt.Sprintf("$%d", *itemNum)
				} else {
					val = q.getWhereValue(item.Val)
				}
			}
		}

		if val == "" {
			wheres = append(wheres, fmt.Sprintf(`%s %s`, item.Column, Type))
		} else {
			wheres = append(wheres, fmt.Sprintf(`%s %s %s`, item.Column, Type, val))
		}
	}

	return wheres
}
func (q *QueryBuilder) getWhereValue(val any) (resp string) {
	switch val.(type) {
	case string:
		resp = fmt.Sprintf("'%s'", val)
	default:
		resp = fmt.Sprint(val)
	}

	return resp
}
func (q *QueryBuilder) setSpanAttribute(key, val string) {
	if q.otelSpan != nil {
		q.otelSpan.SetAttributes(attribute.String(key, val))
	}
}

// Utils

// HasValues verifica se existem valores definidos para serem utilizados em operações de UPDATE.
//
// Retorna true, indicando que há colunas e valores prontos para serem atualizados na query. Caso contrário, retorna false.
//
// Exemplo de uso:
//
//	qb := query.NewQueryBuilder().
//	    From("users").
//	    Values(query.Value{Column: "name", Val: "Novo Nome"})
//
//	if qb.HasValues() {
//	    // Pode executar um UPDATE
//	}
func (q *QueryBuilder) HasValues() bool {
	return len(q.values) != 0
}
