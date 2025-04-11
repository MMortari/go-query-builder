package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestCase struct {
	title  string
	data   *QueryBuilder
	result string
	args   []interface{}
}

func TestNewQueryBuilder(t *testing.T) {
	qb := NewQueryBuilder()

	assert.NotNil(t, qb, "QueryBuilder instance should not be nil")
	assert.Empty(t, qb.from, "QueryBuilder 'from' field should be empty")
	assert.Empty(t, qb.selects, "QueryBuilder 'selects' field should be empty")
	assert.Empty(t, qb.wheresAnd, "QueryBuilder 'wheres' field should be empty")
	assert.Nil(t, qb.limit, "QueryBuilder 'limit' field should be nil")
	assert.Nil(t, qb.offset, "QueryBuilder 'offset' field should be nil")
	assert.Empty(t, qb.orderBys, "QueryBuilder 'orderBys' field should be empty")

	data := []TestCase{
		// From
		{
			title:  "Test From",
			data:   NewQueryBuilder().From("users"),
			result: `SELECT * FROM "users"`,
			args:   []interface{}{},
		},
		{
			title:  "Test From AS",
			data:   NewQueryBuilder().From("users", "u"),
			result: `SELECT * FROM "users" AS "u"`,
			args:   []interface{}{},
		},

		// Select
		{
			title:  "Test Select Empty",
			data:   NewQueryBuilder().From("users"),
			result: `SELECT * FROM "users"`,
			args:   []interface{}{},
		},
		{
			title:  "Test Select Named",
			data:   NewQueryBuilder().From("users").Select("id", "name").WhereAnd(Where{Column: "age", Type: ">", Val: 18}).Limit(10).Offset(5).OrderBy(OrderBy{Column: "name", Type: "ASC"}),
			result: `SELECT id, name FROM "users" WHERE (age > $1) ORDER BY name ASC LIMIT 10 OFFSET 5`,
			args:   []interface{}{18},
		},
		{
			title:  "Test Select *",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "age", Type: "=", Val: 12}).OrderBy(OrderBy{Column: "age"}),
			result: `SELECT * FROM "users" WHERE (age = $1) ORDER BY age`,
			args:   []interface{}{12},
		},

		// Where
		{
			title:  "Test Where String",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}).OrderBy(OrderBy{Column: "age"}),
			result: `SELECT * FROM "users" WHERE (name = $1) ORDER BY age`,
			args:   []interface{}{"Mark"},
		},
		{
			title:  "Test Where Int",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "age", Type: "<=", Val: 18}),
			result: `SELECT * FROM "users" WHERE (age <= $1)`,
			args:   []interface{}{18},
		},
		{
			title:  "Test Where Float",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "salary", Type: ">", Val: 18.96}),
			result: `SELECT * FROM "users" WHERE (salary > $1)`,
			args:   []interface{}{18.96},
		},
		{
			title:  "Test Where Bool",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "is_hired", Type: "=", Val: true}),
			result: `SELECT * FROM "users" WHERE (is_hired = $1)`,
			args:   []interface{}{true},
		},
		{
			title:  "Test Where Multiple",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}).WhereAnd(Where{Column: "age", Type: "<=", Val: 18}).WhereAnd(Where{Column: "salary", Type: ">", Val: 18.96}).WhereAnd(Where{Column: "is_hired", Type: "=", Val: true}),
			result: `SELECT * FROM "users" WHERE (name = $1) AND (age <= $2) AND (salary > $3) AND (is_hired = $4)`,
			args:   []interface{}{"Mark", 18, 18.96, true},
		},
		{
			title:  "Test Where Multiple",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}, Where{Column: "age", Type: "<=", Val: 18}, Where{Column: "salary", Type: ">", Val: 18.96}, Where{Column: "is_hired", Type: "=", Val: true}),
			result: `SELECT * FROM "users" WHERE (name = $1 AND age <= $2 AND salary > $3 AND is_hired = $4)`,
			args:   []interface{}{"Mark", 18, 18.96, true},
		},
		{
			title:  "Test Where Or",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}).WhereOr(Where{Column: "age", Type: "<=", Val: 18}),
			result: `SELECT * FROM "users" WHERE (name = $1) OR (age <= $2)`,
			args:   []interface{}{"Mark", 18},
		},
		{
			title:  "Test Where Multiple Or",
			data:   NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}, Where{Column: "age", Type: "=", Val: 18}).WhereOr(Where{Column: "name", Type: "!=", Val: "James"}, Where{Column: "salary", Type: ">=", Val: 15899.85}).WhereOr(Where{Column: "name", Type: "=", Val: "Joanes"}, Where{Column: "is_hired", Type: "=", Val: true}),
			result: `SELECT * FROM "users" WHERE (name = $1 AND age = $2) OR (name != $3 AND salary >= $4) OR (name = $5 AND is_hired = $6)`,
			args:   []interface{}{"Mark", 18, "James", 15899.85, "Joanes", true},
		},

		// Order By
		{
			title:  "Test Select Order By",
			data:   NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name"}),
			result: `SELECT * FROM "users" ORDER BY name`,
			args:   []interface{}{},
		},
		{
			title:  "Test Select Order By",
			data:   NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name", Type: "ASC"}),
			result: `SELECT * FROM "users" ORDER BY name ASC`,
			args:   []interface{}{},
		},
		{
			title:  "Test Select Order By",
			data:   NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name", Type: "DESC"}),
			result: `SELECT * FROM "users" ORDER BY name DESC`,
			args:   []interface{}{},
		},

		// Pagination Paged
		{
			title:  "Test Select Pagination Paged",
			data:   NewQueryBuilder().From("users").Select("*").PaginationPaged(1, 25),
			result: `SELECT * FROM "users" LIMIT 25 OFFSET 0`,
			args:   []interface{}{},
		},
	}

	for _, item := range data {
		t.Run(item.title, func(t *testing.T) {
			query, args := item.data.ToSelectSql()

			assert.Equalf(t, query, item.result, "Invalid query")
			assert.Equalf(t, args, item.args, "Invalid args")
		})
	}
}
