# go-query-builder

**go-query-builder** é uma biblioteca Go para construção dinâmica e segura de queries SQL, com suporte a SELECT, UPDATE, JOINs, WHEREs complexos, paginação, ordenação, agrupamento e integração opcional com OpenTelemetry para rastreamento de queries.

## Objetivo

Facilitar a montagem de queries SQL de forma programática, evitando SQL injection, reduzindo repetição de código e tornando a manipulação de queries mais legível e flexível.

---

## Exemplo de Uso

### Select

```go
qb := query.NewQueryBuilder().
  From("users", "u").
  Select("u.id", "u.name", "p.phone").
  Join(query.Join{
    Table: "phones",
    As:    "p",
    On:    "p.user_id = u.id",
    Type:  query.LeftJoin,
  }).
  WhereAnd(
    query.Where{Column: "u.active", Type: "=", Val: true},
    query.Where{Column: "u.age", Type: ">", Val: 18},
  ).
  WhereOr(
    query.Where{Column: "u.country", Type: "IN", Val: []string{"BR", "US"}},
  ).
  OrderBy(query.OrderBy{Column: "u.name", Type: "asc"}).
  PaginationPaged(2, 10) // página 2, 10 resultados por página

sql, params := qb.ToSelectSql()
// SQL: SELECT u.id, u.name, p.phone FROM "users" AS "u" LEFT JOIN "phones" AS "p" ON p.user_id = u.id WHERE ((u.active = $1 AND u.age > $2) OR (u.country IN ($3, $4))) ORDER BY u.name ASC LIMIT 10 OFFSET 10
// Parâmetros: [true 18 BR US]

sql, params := qb.ToSelectTotalSql()
// SQL: SELECT COUNT(*) FROM "users" AS "u" LEFT JOIN "phones" AS "p" ON p.user_id = u.id WHERE ((u.active = $1 AND u.age > $2) OR (u.country IN ($3, $4)))
// Parâmetros: [true 18 BR US]
```

### Update

```go
qb := query.NewQueryBuilder().
  From("users").
  Values(
    query.Value{Column: "name", Val: "Novo Nome"},
    query.Value{Column: "active", Val: false},
  ).
  WhereAnd(
    query.Where{Column: "id", Type: "=", Val: 123},
  )

sql, params := qb.ToUpdateQuery()
// SQL: UPDATE "users" SET name = $1, active = $2 WHERE (id = $3)
// Parâmetros: [Novo Nome false 123]
```

---

## Principais Componentes

### Tipos de Dados

- **Where**  
  Representa uma condição de filtro (`WHERE`).

  - `Column`: Nome da coluna.
  - `Type`: Tipo de comparação (ex: `=`, `IN`, `LIKE`).
  - `Val`: Valor a ser comparado.

- **Value**  
  Usado para valores em operações de atualização (`UPDATE`).

  - `Column`: Nome da coluna.
  - `Val`: Novo valor.

- **OrderBy**  
  Define ordenação dos resultados.

  - `Column`: Nome da coluna.
  - `Type`: Tipo de ordenação (`ASC`, `DESC`).

- **Join**  
  Representa um JOIN em uma query.
  - `Table`: Nome da tabela a ser unida.
  - `As`: Alias da tabela.
  - `On`: Condição do JOIN.
  - `Type`: Tipo do JOIN, enum para tipos de JOIN: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`..

---

### Métodos Principais

- **NewQueryBuilder**  
  Cria uma nova instância do builder.

- **From**  
  Define a tabela principal (e alias, se necessário).

- **Select**  
  Define as colunas a serem selecionadas.

- **Values**  
  Define valores para UPDATE.

- **Join**  
  Adiciona um JOIN à query.

- **WhereAnd / WhereOr**  
  Adiciona condições WHERE (AND/OR).

- **PaginationPaged, Limit, Offset**  
  Define paginação.

- **OrderBy, ClearOrderBy**  
  Define ou limpa ordenação.

- **GroupBy**  
  Define agrupamento.

- **ToSelectSql**  
  Gera a query SELECT final e os parâmetros.

- **ToSelectTotalSql**  
  Gera uma query SELECT para contagem total.

- **ToUpdateQuery**  
  Gera a query UPDATE final e os parâmetros.

- **HasValues**  
  Verifica se há valores definidos para UPDATE.

---

## Observações

- Os métodos retornam o próprio builder, permitindo encadeamento (fluent interface).
- O uso de parâmetros (`$1`, `$2`, ...) previne SQL injection.
- Suporte a JOINs, WHEREs complexos (AND/OR), paginação, ordenação e agrupamento.
- Integração opcional com OpenTelemetry para rastreamento de queries.
