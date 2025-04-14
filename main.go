package query

import (
	"fmt"
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
	query strings.Builder

	from      string
	selects   []string
	joins     []Join
	wheresAnd [][]Where
	wheresOr  [][]Where
	limit     *int
	offset    *int
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

func (q *QueryBuilder) ToSelectSql() (query string, queryData []interface{}) {
	// SELECT
	q.query.WriteString("SELECT ")
	if len(q.selects) == 0 {
		q.query.WriteString("*")
	} else {
		q.query.WriteString(strings.Join(q.selects, ", "))
	}
	q.query.WriteString(" ")

	// FROM
	q.query.WriteString(fmt.Sprintf(`FROM %s`, q.from))

	// JOIN
	if len(q.joins) != 0 {
		for _, item := range q.joins {
			joinType := InnerJoin

			if item.Type != "" {
				joinType = item.Type
			}

			q.query.WriteString(fmt.Sprintf(` %s "%s" AS "%s" ON %s`, joinType, item.Table, item.As, item.On))
		}
	}

	// WHERE
	var where string
	where, queryData = q.getWhere()
	q.query.WriteString(where)

	// ORDER BY
	if len(q.orderBys) != 0 {
		q.query.WriteString(" ORDER BY ")
		orderBy := make([]string, 0, len(q.orderBys))

		for _, item := range q.orderBys {
			if item.Type == "" {
				orderBy = append(orderBy, item.Column)
			} else {
				orderBy = append(orderBy, fmt.Sprintf("%s %s", item.Column, strings.ToUpper(item.Type)))
			}
		}

		q.query.WriteString(strings.Join(orderBy, ", "))
	}
	// LIMIT
	if q.limit != nil {
		q.query.WriteString(fmt.Sprintf(" LIMIT %d", *q.limit))
	}
	// OFFSET
	if q.offset != nil {
		q.query.WriteString(fmt.Sprintf(" OFFSET %d", *q.offset))
	}

	query = q.query.String()

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
			wheres := make([]string, 0, len(q.wheresAnd))

			for _, item := range whereAnd {
				itemNum++
				queryData = append(queryData, item.Val)
				switch item.Val.(type) {
				case string:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				case float32, float64:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				case bool:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				}
			}

			whereAndBuilder = append(whereAndBuilder, fmt.Sprintf("(%s)", strings.Join(wheres, " AND ")))
		}
		wheresToOr = append(wheresToOr, strings.Join(whereAndBuilder, " AND "))
	}
	if len(q.wheresOr) != 0 {
		for _, whereAnd := range q.wheresOr {
			wheres := make([]string, 0, len(q.wheresOr))

			for _, item := range whereAnd {
				itemNum++
				queryData = append(queryData, item.Val)
				switch item.Val.(type) {
				case string:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				case float32, float64:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				case bool:
					wheres = append(wheres, fmt.Sprintf(`%s %s $%d`, item.Column, item.Type, itemNum))
				}
			}

			wheresToOr = append(wheresToOr, fmt.Sprintf("(%s)", strings.Join(wheres, " AND ")))
		}
	}

	qb.WriteString(strings.Join(wheresToOr, " OR "))

	return qb.String(), queryData
}
