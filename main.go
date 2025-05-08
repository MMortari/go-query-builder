package query

import (
	"fmt"
	"reflect"
	"strings"
)

type Where struct {
	Column string
	Type   string
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
	from      string
	selects   []string
	joins     []Join
	wheresAnd [][]Where
	wheresOr  [][]Where
	limit     *int
	offset    *int
	groupBy   []string
	orderBys  []OrderBy
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (q *QueryBuilder) From(from ...string) *QueryBuilder {
	if len(from) == 1 {
		q.from = fmt.Sprintf(`"%s"`, from[0])
	}
	if len(from) == 2 {
		q.from = fmt.Sprintf(`"%s" AS "%s"`, from[0], from[1])
	}
	return q
}
func (q *QueryBuilder) Select(selects ...string) *QueryBuilder {
	q.selects = selects
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
	where, queryData = q.getWhere()
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
	where, queryData = q.getWhere()
	qb.WriteString(where)

	query = qb.String()
	qb = strings.Builder{}

	return query, queryData
}

func (q *QueryBuilder) getWhere() (string, []interface{}) {
	queryData := make([]interface{}, 0)

	if len(q.wheresOr) == 0 && len(q.wheresAnd) == 0 {
		return "", queryData
	}

	qb := strings.Builder{}

	qb.WriteString(" WHERE ")
	itemNum := 0

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

					(*itemNum)++
					*queryData = append(*queryData, value)
					values = append(values, fmt.Sprintf(" $%d", *itemNum))
				}
				val = strings.Join(values, " AND")
			} else {
				(*itemNum)++
				*queryData = append(*queryData, item.Val)
				val = fmt.Sprintf(" $%d", *itemNum)
			}
		}

		wheres = append(wheres, fmt.Sprintf(`%s %s%s`, item.Column, Type, val))
	}

	return wheres
}
