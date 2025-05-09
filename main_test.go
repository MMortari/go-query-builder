package query

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
)

type TestCase struct {
	title       string
	data        *QueryBuilder
	result      string
	resultTotal string
	args        []interface{}
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
			title:       "Test From",
			data:        NewQueryBuilder().From("users"),
			result:      `SELECT * FROM "users"`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},
		{
			title:       "Test From AS",
			data:        NewQueryBuilder().From("users", "u"),
			result:      `SELECT * FROM "users" AS "u"`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" AS "u"`,
			args:        []interface{}{},
		},

		// Select
		{
			title:       "Test Select Empty",
			data:        NewQueryBuilder().From("users"),
			result:      `SELECT * FROM "users"`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},
		{
			title:       "Test Select Named",
			data:        NewQueryBuilder().From("users").Select("id", "name").WhereAnd(Where{Column: "age", Type: ">", Val: 18}).Limit(10).Offset(5).OrderBy(OrderBy{Column: "name", Type: "ASC"}),
			result:      `SELECT id, name FROM "users" WHERE (age > $1) ORDER BY name ASC LIMIT 10 OFFSET 5`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age > $1)`,
			args:        []interface{}{18},
		},
		{
			title:       "Test Select *",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "age", Type: "=", Val: 12}).OrderBy(OrderBy{Column: "age"}),
			result:      `SELECT * FROM "users" WHERE (age = $1) ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age = $1)`,
			args:        []interface{}{12},
		},
		{
			title:       "Test Clear Select 1",
			data:        NewQueryBuilder().From("users").Select("id", "name").WhereAnd(Where{Column: "age", Type: "=", Val: 12}).OrderBy(OrderBy{Column: "age"}).ClearSelect(),
			result:      `SELECT * FROM "users" WHERE (age = $1) ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age = $1)`,
			args:        []interface{}{12},
		},
		{
			title:       "Test Clear Select 2",
			data:        NewQueryBuilder().From("users").Select("id", "name").WhereAnd(Where{Column: "age", Type: "=", Val: 12}).OrderBy(OrderBy{Column: "age"}).ClearSelect().Select("age"),
			result:      `SELECT age FROM "users" WHERE (age = $1) ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age = $1)`,
			args:        []interface{}{12},
		},

		// Join
		{
			title:       "Test Join Type Default",
			data:        NewQueryBuilder().From("users", "u").Join(Join{Table: "event", As: "e", On: `"u"."id_event" = "e"."id_event"`}),
			result:      `SELECT * FROM "users" AS "u" INNER JOIN "event" AS "e" ON "u"."id_event" = "e"."id_event"`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" AS "u" INNER JOIN "event" AS "e" ON "u"."id_event" = "e"."id_event"`,
			args:        []interface{}{},
		},
		{
			title:       "Test Join Type Right Join",
			data:        NewQueryBuilder().From("users", "u").Join(Join{Table: "event", As: "e", On: `"u"."id_event" = "e"."id_event"`, Type: RightJoin}),
			result:      `SELECT * FROM "users" AS "u" RIGHT JOIN "event" AS "e" ON "u"."id_event" = "e"."id_event"`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" AS "u" RIGHT JOIN "event" AS "e" ON "u"."id_event" = "e"."id_event"`,
			args:        []interface{}{},
		},
		{
			title:       "Test Join Multiple",
			data:        NewQueryBuilder().From("users", "u").Join(Join{Table: "event", As: "e", On: `"u"."id_event" = "e"."id_event"`}).Join(Join{Table: "address", As: "a", On: `"e"."id_address" = "a"."id_address"`, Type: RightJoin}),
			result:      `SELECT * FROM "users" AS "u" INNER JOIN "event" AS "e" ON "u"."id_event" = "e"."id_event" RIGHT JOIN "address" AS "a" ON "e"."id_address" = "a"."id_address"`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" AS "u" INNER JOIN "event" AS "e" ON "u"."id_event" = "e"."id_event" RIGHT JOIN "address" AS "a" ON "e"."id_address" = "a"."id_address"`,
			args:        []interface{}{},
		},

		// Where
		{
			title:       "Test Where String",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}).OrderBy(OrderBy{Column: "age"}),
			result:      `SELECT * FROM "users" WHERE (name = $1) ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name = $1)`,
			args:        []interface{}{"Mark"},
		},
		{
			title:       "Test Where String Like",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "like", Val: "_Mark%"}).OrderBy(OrderBy{Column: "age"}),
			result:      `SELECT * FROM "users" WHERE (name LIKE $1) ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name LIKE $1)`,
			args:        []interface{}{"_Mark%"},
		},
		{
			title:       "Test Where String ILike",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "ilike", Val: "%Mark%"}).OrderBy(OrderBy{Column: "age"}),
			result:      `SELECT * FROM "users" WHERE (name ILIKE $1) ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name ILIKE $1)`,
			args:        []interface{}{"%Mark%"},
		},
		{
			title:       "Test Where Int",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "age", Type: "<=", Val: 18}),
			result:      `SELECT * FROM "users" WHERE (age <= $1)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age <= $1)`,
			args:        []interface{}{18},
		},
		{
			title:       "Test Where Float",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "salary", Type: ">", Val: 18.96}),
			result:      `SELECT * FROM "users" WHERE (salary > $1)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (salary > $1)`,
			args:        []interface{}{18.96},
		},
		{
			title:       "Test Where Bool",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "is_hired", Type: "=", Val: true}),
			result:      `SELECT * FROM "users" WHERE (is_hired = $1)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (is_hired = $1)`,
			args:        []interface{}{true},
		},
		{
			title:       "Test Where Multiple",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}).WhereAnd(Where{Column: "age", Type: "<=", Val: 18}).WhereAnd(Where{Column: "salary", Type: ">", Val: 18.96}).WhereAnd(Where{Column: "is_hired", Type: "=", Val: true}),
			result:      `SELECT * FROM "users" WHERE (name = $1) AND (age <= $2) AND (salary > $3) AND (is_hired = $4)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name = $1) AND (age <= $2) AND (salary > $3) AND (is_hired = $4)`,
			args:        []interface{}{"Mark", 18, 18.96, true},
		},
		{
			title:       "Test Where Multiple",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}, Where{Column: "age", Type: "<=", Val: 18}, Where{Column: "salary", Type: ">", Val: 18.96}, Where{Column: "is_hired", Type: "=", Val: true}),
			result:      `SELECT * FROM "users" WHERE (name = $1 AND age <= $2 AND salary > $3 AND is_hired = $4)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name = $1 AND age <= $2 AND salary > $3 AND is_hired = $4)`,
			args:        []interface{}{"Mark", 18, 18.96, true},
		},
		{
			title:       "Test Where Or",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}).WhereOr(Where{Column: "age", Type: "<=", Val: 18}),
			result:      `SELECT * FROM "users" WHERE (name = $1) OR (age <= $2)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name = $1) OR (age <= $2)`,
			args:        []interface{}{"Mark", 18},
		},
		{
			title:       "Test Where Multiple Or",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "name", Type: "=", Val: "Mark"}, Where{Column: "age", Type: "=", Val: 18}).WhereOr(Where{Column: "name", Type: "!=", Val: "James"}, Where{Column: "salary", Type: ">=", Val: 15899.85}).WhereOr(Where{Column: "name", Type: "=", Val: "Joanes"}, Where{Column: "is_hired", Type: "=", Val: true}),
			result:      `SELECT * FROM "users" WHERE (name = $1 AND age = $2) OR (name != $3 AND salary >= $4) OR (name = $5 AND is_hired = $6)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name = $1 AND age = $2) OR (name != $3 AND salary >= $4) OR (name = $5 AND is_hired = $6)`,
			args:        []interface{}{"Mark", 18, "James", 15899.85, "Joanes", true},
		},
		{
			title:       "Test Where Between",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "age", Type: "between", Val: []int{10, 20}}),
			result:      `SELECT * FROM "users" WHERE (age BETWEEN $1 AND $2)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age BETWEEN $1 AND $2)`,
			args:        []interface{}{10, 20},
		},
		{
			title:       "Test Where IS NULL",
			data:        NewQueryBuilder().From("users").Select("*").WhereAnd(Where{Column: "age", Type: "is null"}),
			result:      `SELECT * FROM "users" WHERE (age IS NULL)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age IS NULL)`,
			args:        []interface{}{},
		},

		// Group By
		{
			title:       "Test Group By",
			data:        NewQueryBuilder().From("users").Select("age", "COUNT(salary)", "SUM(salary)").WhereAnd(Where{Column: "age", Type: "=", Val: 12}).GroupBy("age").OrderBy(OrderBy{Column: "age"}),
			result:      `SELECT age, COUNT(salary), SUM(salary) FROM "users" WHERE (age = $1) GROUP BY age ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age = $1)`,
			args:        []interface{}{12},
		},
		{
			title:       "Test Group By Multiple",
			data:        NewQueryBuilder().From("users").Select("age", "COUNT(salary)", "SUM(salary)").WhereAnd(Where{Column: "age", Type: "=", Val: 12}).GroupBy("age", "name", "id").OrderBy(OrderBy{Column: "age"}),
			result:      `SELECT age, COUNT(salary), SUM(salary) FROM "users" WHERE (age = $1) GROUP BY age, name, id ORDER BY age`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (age = $1)`,
			args:        []interface{}{12},
		},

		// Order By
		{
			title:       "Test Select Order By",
			data:        NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name"}),
			result:      `SELECT * FROM "users" ORDER BY name`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},
		{
			title:       "Test Select Order By",
			data:        NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name", Type: "ASC"}),
			result:      `SELECT * FROM "users" ORDER BY name ASC`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},
		{
			title:       "Test Select Order By",
			data:        NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name", Type: "DESC"}),
			result:      `SELECT * FROM "users" ORDER BY name DESC`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},
		{
			title:       "Test Select Clear Order By",
			data:        NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name", Type: "DESC"}).ClearOrderBy(),
			result:      `SELECT * FROM "users"`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},
		{
			title:       "Test Select Clear Order By",
			data:        NewQueryBuilder().From("users").Select("*").OrderBy(OrderBy{Column: "name", Type: "DESC"}).ClearOrderBy().OrderBy(OrderBy{Column: "age", Type: "DESC"}),
			result:      `SELECT * FROM "users" ORDER BY age DESC`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},

		// Pagination Paged
		{
			title:       "Test Select Pagination Paged",
			data:        NewQueryBuilder().From("users").Select("*").PaginationPaged(1, 25),
			result:      `SELECT * FROM "users" LIMIT 25 OFFSET 0`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users"`,
			args:        []interface{}{},
		},

		// Config
		{
			title: "Test Parse Where false",
			data: NewQueryBuilder(ParseWhere(false)).From("users").WhereAnd(
				Where{Column: "name", Type: "=", Val: "Mark"},
				Where{Column: "age", Type: "=", Val: int(18)},
				Where{Column: "salary", Type: "=", Val: float64(15000.50)},
				Where{Column: "active", Type: "=", Val: true},
				Where{Column: "permission", Type: "in", Val: []any{"admin", "user", int(18), float64(15000.50), false}},
				Where{Column: "distance", Type: "between", Val: []any{0, 100}},
				Where{Column: "is_hired", Type: "is null"},
			),
			result:      `SELECT * FROM "users" WHERE (name = 'Mark' AND age = 18 AND salary = 15000.50 AND active = true AND permission IN ('admin', 'user', 18, 15000.50, false) AND distance BETWEEN 0 AND 100 AND is_hired IS NULL)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name = 'Mark' AND age = 18 AND salary = 15000.50 AND active = true AND permission IN ('admin', 'user', 18, 15000.50, false) AND distance BETWEEN 0 AND 100 AND is_hired IS NULL)`,
			args:        []interface{}{},
		},
		{
			title: "Test Parse Where true",
			data: NewQueryBuilder(ParseWhere(true)).From("users").WhereAnd(
				Where{Column: "name", Type: "=", Val: "Mark"},
				Where{Column: "age", Type: "=", Val: int(18)},
				Where{Column: "salary", Type: "=", Val: float64(15000.50)},
				Where{Column: "active", Type: "=", Val: true},
				Where{Column: "permission", Type: "in", Val: []any{"admin", "user", int(18), float64(15000.50), false}},
				Where{Column: "distance", Type: "between", Val: []any{0, 100}},
				Where{Column: "is_hired", Type: "is null"},
			),
			result:      `SELECT * FROM "users" WHERE (name = $1 AND age = $2 AND salary = $3 AND active = $4 AND permission IN ($5, $6, $7, $8, $9) AND distance BETWEEN $10 AND $11 AND is_hired IS NULL)`,
			resultTotal: `SELECT COUNT(*) AS total FROM "users" WHERE (name = $1 AND age = $2 AND salary = $3 AND active = $4 AND permission IN ($5, $6, $7, $8, $9) AND distance BETWEEN $10 AND $11 AND is_hired IS NULL)`,
			args:        []interface{}{"Mark", 18, 15000.5, true, "admin", "user", 18, 15000.5, false, 0, 100},
		},
	}

	for _, item := range data {
		t.Run(item.title, func(t *testing.T) {
			query, args := item.data.ToSelectSql()

			assert.Equalf(t, query, item.result, "Invalid query")
			assert.Equalf(t, args, item.args, "Invalid args")

			if item.resultTotal != "" {
				queryTotal, argsTotal := item.data.ToSelectTotalSql()

				assert.Equalf(t, queryTotal, item.resultTotal, "Invalid query")
				assert.Equalf(t, argsTotal, item.args, "Invalid args")
			}

			// Running a third-party query parse to validate the query to improve confiability
			_, err := pg_query.Parse(item.result)
			assert.NoError(t, err)
		})
	}
}
