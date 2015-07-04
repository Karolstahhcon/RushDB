//! Complex SQL aggregation and join qualification.
//!
//! Exercises multi-table joins over labels and edge tables, grouped
//! aggregations, HAVING, DISTINCT counts, window functions, CTEs, subqueries,
//! NULL semantics, arrays, and join fan-out. Expected values are computed by
//! hand from the fixture so a wrong result fails rather than merely differing.

use std::sync::Arc;

use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::{Lmpg, RecordRef};
use rushdb_sql::{execute_sql, Row};
use rushdb_storage::Store;
use rushdb_types::ids::TenantId;
use serde_json::{json, Value};

struct Fixture {
    graph: Lmpg,
    #[allow(dead_code)]
    persons: Vec<RecordRef>,
}

#[expect(
    clippy::too_many_arguments,
    reason = "linear test-fixture builder; one argument per property keeps the table readable"
)]
fn person(
    graph: &Lmpg,
    name: &str,
    age: i64,
    salary: i64,
    dept: &str,
    active: bool,
    nick: Value,
    tags: Value,
    rating: f64,
) -> RecordRef {
    graph
        .create_record(
            "Person",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!(name))),
                ("age".into(), FieldChange::Set(json!(age))),
                ("salary".into(), FieldChange::Set(json!(salary))),
                ("department".into(), FieldChange::Set(json!(dept))),
                ("active".into(), FieldChange::Set(json!(active))),
                ("nickname".into(), FieldChange::Set(nick)),
                ("tags".into(), FieldChange::Set(tags)),
                ("rating".into(), FieldChange::Set(json!(rating))),
            ],
        )
        .unwrap()
}

#[expect(
    clippy::too_many_lines,
    reason = "single linear fixture builder; splitting it would obscure the graph shape"
)]
fn fixture() -> Fixture {
    let graph = Lmpg::new(Arc::new(Store::new_in_memory()), TenantId::from_ordinal(7));

    let acme = graph
        .create_record(
            "Company",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!("Acme"))),
                ("industry".into(), FieldChange::Set(json!("tech"))),
            ],
        )
        .unwrap();
    let globex = graph
        .create_record(
            "Company",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!("Globex"))),
                ("industry".into(), FieldChange::Set(json!("tech"))),
            ],
        )
        .unwrap();
    // A company with no employees, for LEFT JOIN count semantics.
    let _initech = graph
        .create_record(
            "Company",
            None,
            vec![
                ("name".into(), FieldChange::Set(json!("Initech"))),
                ("industry".into(), FieldChange::Set(json!("services"))),
            ],
        )
        .unwrap();
    let apollo = graph
        .create_record(
            "Project",
            None,
            vec![("name".into(), FieldChange::Set(json!("Apollo")))],
        )
        .unwrap();
    let gemini = graph
        .create_record(
            "Project",
            None,
            vec![("name".into(), FieldChange::Set(json!("Gemini")))],
        )
        .unwrap();

    let alice = person(
        &graph,
        "Alice",
        30,
        100_000,
        "engineering",
        true,
        json!("Al"),
        json!(["a", "b"]),
        4.5,
    );
    let bob = person(
        &graph,
        "Bob",
        25,
        90_000,
        "engineering",
        false,
        Value::Null,
        json!(["b"]),
        3.0,
    );
    let carol = person(
        &graph,
        "Carol",
        40,
        150_000,
        "research",
        true,
        json!("C"),
        json!(["c"]),
        4.9,
    );
    let dave = person(
        &graph,
        "Dave",
        35,
        120_000,
        "research",
        true,
        Value::Null,
        json!([]),
        3.5,
    );
    let eve = person(
        &graph,
        "Eve",
        28,
        95_000,
        "sales",
        false,
        json!("Ev"),
        json!(["a"]),
        2.5,
    );
    let frank = person(
        &graph,
        "Frank",
        50,
        200_000,
        "sales",
        true,
        Value::Null,
        json!(["d", "e", "f"]),
        5.0,
    );

    for (person_ref, company, since) in [
        (alice, acme, 2015),
        (bob, acme, 2018),
        (carol, globex, 2012),
        (dave, globex, 2019),
        (eve, acme, 2021),
    ] {
        graph
            .create_relationship(
                "WORKS_AT",
                person_ref,
                company,
                vec![("since".into(), FieldChange::Set(json!(since)))],
            )
            .unwrap();
    }
    for (from, to) in [(alice, bob), (bob, carol), (carol, dave), (alice, carol)] {
        graph
            .create_relationship("KNOWS", from, to, vec![])
            .unwrap();
    }
    for (person_ref, project) in [(alice, apollo), (carol, apollo), (dave, gemini)] {
        graph
            .create_relationship("ASSIGNED", person_ref, project, vec![])
            .unwrap();
    }

    Fixture {
        graph,
        persons: vec![alice, bob, carol, dave, eve, frank],
    }
}

fn rows(graph: &Lmpg, sql: &str) -> Vec<Row> {
    execute_sql(graph, sql).unwrap_or_else(|error| panic!("`{sql}` failed: {error}"))
}

fn one(graph: &Lmpg, sql: &str) -> Row {
    let mut result = rows(graph, sql);
    assert_eq!(result.len(), 1, "expected one row for `{sql}`: {result:?}");
    result.pop().unwrap()
}

#[test]
fn group_by_aggregates_are_exact() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT department, COUNT(*) AS c, SUM(salary) AS s, AVG(salary) AS a, MIN(age) AS mn, MAX(age) AS mx \
         FROM person GROUP BY department ORDER BY department",
    );
    assert_eq!(result.len(), 3);
    assert_eq!(result[0]["department"], json!("engineering"));
    assert_eq!(result[0]["c"], json!(2));
    assert_eq!(result[0]["s"], json!(190_000));
    assert_eq!(result[0]["a"], json!(95_000.0));
    assert_eq!(result[0]["mn"], json!(25));
    assert_eq!(result[0]["mx"], json!(30));
    assert_eq!(result[1]["department"], json!("research"));
    assert_eq!(result[1]["s"], json!(270_000));
    assert_eq!(result[2]["department"], json!("sales"));
    assert_eq!(result[2]["s"], json!(295_000));
}

#[test]
fn having_filters_groups() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT department FROM person GROUP BY department HAVING COUNT(*) > 2",
    );
    assert!(result.is_empty(), "no department has >2 people: {result:?}");
    let result = rows(
        &f.graph,
        "SELECT department FROM person GROUP BY department HAVING SUM(salary) > 250000 ORDER BY department",
    );
    let depts: Vec<&str> = result
        .iter()
        .map(|r| r["department"].as_str().unwrap())
        .collect();
    assert_eq!(depts, vec!["research", "sales"]);
}

#[test]
fn distinct_counts_and_nulls() {
    let f = fixture();
    let result = one(
        &f.graph,
        "SELECT COUNT(DISTINCT department) AS d FROM person",
    );
    assert_eq!(result["d"], json!(3));

    let result = one(
        &f.graph,
        "SELECT COUNT(nickname) AS named, COUNT(*) AS total FROM person",
    );
    assert_eq!(result["named"], json!(3), "three non-null nicknames");
    assert_eq!(result["total"], json!(6));

    let result = one(
        &f.graph,
        "SELECT SUM(CASE WHEN nickname IS NULL THEN 1 ELSE 0 END) AS missing FROM person",
    );
    assert_eq!(result["missing"], json!(3));
}

#[test]
fn float_aggregate_over_mixed_numeric_column() {
    let f = fixture();
    let result = one(&f.graph, "SELECT AVG(rating) AS avg_rating FROM person");
    let avg = result["avg_rating"].as_f64().unwrap();
    assert!((avg - 3.9).abs() < 1e-9, "expected 3.9, got {avg}");
}

#[test]
fn left_join_preserves_unmatched_companies() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT c.name AS company, COUNT(w.__id) AS n, COUNT(*) AS rows \
         FROM company c LEFT JOIN rushdb_edges.works_at w ON c.__id = w.__target_id \
         GROUP BY c.name ORDER BY c.name",
    );
    assert_eq!(result.len(), 3);
    assert_eq!(result[0]["company"], json!("Acme"));
    assert_eq!(result[0]["n"], json!(3));
    assert_eq!(result[0]["rows"], json!(3));
    assert_eq!(result[1]["company"], json!("Globex"));
    assert_eq!(result[1]["n"], json!(2));
    // Initech has no employees: COUNT(col) is 0 but COUNT(*) is 1 (the null row).
    assert_eq!(result[2]["company"], json!("Initech"));
    assert_eq!(result[2]["n"], json!(0));
    assert_eq!(result[2]["rows"], json!(1));
}

#[test]
fn multi_join_grouped_aggregate() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT c.name AS company, COUNT(*) AS n, AVG(p.salary) AS avg_salary \
         FROM person p \
         JOIN rushdb_edges.works_at w ON p.__id = w.__source_id \
         JOIN company c ON c.__id = w.__target_id \
         GROUP BY c.name ORDER BY c.name",
    );
    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["company"], json!("Acme"));
    assert_eq!(result[0]["n"], json!(3));
    assert_eq!(result[0]["avg_salary"], json!(95_000.0));
    assert_eq!(result[1]["company"], json!("Globex"));
    assert_eq!(result[1]["avg_salary"], json!(135_000.0));
}

#[test]
fn self_join_through_edges() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT a.name AS src, b.name AS dst \
         FROM person a \
         JOIN rushdb_edges.knows e ON a.__id = e.__source_id \
         JOIN person b ON b.__id = e.__target_id \
         ORDER BY a.name, b.name",
    );
    let pairs: Vec<(String, String)> = result
        .iter()
        .map(|r| {
            (
                r["src"].as_str().unwrap().to_string(),
                r["dst"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Bob".to_string()),
            ("Alice".to_string(), "Carol".to_string()),
            ("Bob".to_string(), "Carol".to_string()),
            ("Carol".to_string(), "Dave".to_string()),
        ]
    );
}

#[test]
fn two_hop_join_through_edges() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT a.name AS a, c.name AS c \
         FROM person a \
         JOIN rushdb_edges.knows e1 ON a.__id = e1.__source_id \
         JOIN person b ON b.__id = e1.__target_id \
         JOIN rushdb_edges.knows e2 ON b.__id = e2.__source_id \
         JOIN person c ON c.__id = e2.__target_id \
         ORDER BY a.name, c.name",
    );
    let pairs: Vec<(String, String)> = result
        .iter()
        .map(|r| {
            (
                r["a"].as_str().unwrap().to_string(),
                r["c"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Carol".to_string()),
            ("Alice".to_string(), "Dave".to_string()),
            ("Bob".to_string(), "Dave".to_string()),
        ]
    );
}

#[test]
fn cte_join_and_aggregate() {
    let f = fixture();
    let result = one(
        &f.graph,
        "WITH eng AS (SELECT * FROM person WHERE department = 'engineering') \
         SELECT COUNT(*) AS n, SUM(salary) AS s FROM eng",
    );
    assert_eq!(result["n"], json!(2));
    assert_eq!(result["s"], json!(190_000));
}

#[test]
fn window_rank_partitioned_by_department() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT name, department, \
                RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS r \
         FROM person ORDER BY department, r",
    );
    let ordered: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            (
                r["name"].as_str().unwrap().to_string(),
                r["r"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        ordered,
        vec![
            ("Alice".to_string(), 1),
            ("Bob".to_string(), 2),
            ("Carol".to_string(), 1),
            ("Dave".to_string(), 2),
            ("Frank".to_string(), 1),
            ("Eve".to_string(), 2),
        ]
    );
}

#[test]
fn subquery_in_and_exists() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT name FROM person WHERE department IN \
         (SELECT department FROM person GROUP BY department HAVING AVG(salary) > 130000) \
         ORDER BY name",
    );
    let names: Vec<&str> = result.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["Carol", "Dave", "Eve", "Frank"]);

    let result = rows(
        &f.graph,
        "SELECT DISTINCT a.name FROM person a WHERE EXISTS \
         (SELECT 1 FROM rushdb_edges.knows e WHERE e.__source_id = a.__id) ORDER BY a.name",
    );
    let names: Vec<&str> = result.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

#[test]
fn anti_join_not_exists_finds_isolated_person() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT p.name FROM person p WHERE NOT EXISTS \
         (SELECT 1 FROM rushdb_edges.works_at w WHERE w.__source_id = p.__id) ORDER BY p.name",
    );
    let names: Vec<&str> = result.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["Frank"]);
}

#[test]
fn left_join_null_extension_finds_unmatched() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT p.name FROM person p \
         LEFT JOIN rushdb_edges.works_at w ON p.__id = w.__source_id \
         WHERE w.__id IS NULL ORDER BY p.name",
    );
    let names: Vec<&str> = result.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["Frank"]);
}

#[test]
fn array_property_length() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT name, array_length(tags, 1) AS n FROM person ORDER BY name",
    );
    let lengths: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            (
                r["name"].as_str().unwrap().to_string(),
                r["n"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        lengths,
        vec![
            ("Alice".to_string(), 2),
            ("Bob".to_string(), 1),
            ("Carol".to_string(), 1),
            ("Dave".to_string(), 0),
            ("Eve".to_string(), 1),
            ("Frank".to_string(), 3),
        ]
    );
}

#[test]
fn join_fan_out_count_distinct_is_correct() {
    let f = fixture();
    let result = one(
        &f.graph,
        "SELECT COUNT(DISTINCT p.name) AS distinct_sources \
         FROM person p JOIN rushdb_edges.knows e ON p.__id = e.__source_id",
    );
    assert_eq!(result["distinct_sources"], json!(3));
}

#[test]
fn edge_filtered_join_aggregate() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT c.name AS company, COUNT(*) AS n \
         FROM rushdb_edges.works_at w JOIN company c ON c.__id = w.__target_id \
         WHERE w.since >= 2018 GROUP BY c.name ORDER BY c.name",
    );
    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["company"], json!("Acme"));
    assert_eq!(result[0]["n"], json!(2));
    assert_eq!(result[1]["company"], json!("Globex"));
    assert_eq!(result[1]["n"], json!(1));
}

#[test]
fn cross_join_cardinality() {
    let f = fixture();
    let result = one(
        &f.graph,
        "SELECT COUNT(*) AS n FROM person CROSS JOIN company",
    );
    assert_eq!(result["n"], json!(18));
}

#[test]
fn aggregate_over_empty_input_is_zero_and_null() {
    let f = fixture();
    let result = one(
        &f.graph,
        "SELECT COUNT(*) AS n, COUNT(salary) AS ns, SUM(salary) AS s, AVG(salary) AS a \
         FROM person WHERE department = 'nonexistent'",
    );
    assert_eq!(result["n"], json!(0));
    assert_eq!(result["ns"], json!(0));
    assert_eq!(result["s"], Value::Null, "SUM over no rows is NULL");
    assert_eq!(result["a"], Value::Null, "AVG over no rows is NULL");
}

#[test]
fn join_fan_out_double_counts_sum() {
    let f = fixture();
    // Alice has two outgoing KNOWS edges, so her salary is summed twice.
    let result = one(
        &f.graph,
        "SELECT SUM(p.salary) AS s FROM person p \
         JOIN rushdb_edges.knows e ON p.__id = e.__source_id",
    );
    assert_eq!(result["s"], json!(100_000 * 2 + 90_000 + 150_000));
}

#[test]
fn window_running_total() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT name, \
                SUM(salary) OVER (PARTITION BY department ORDER BY salary) AS running \
         FROM person ORDER BY department, salary",
    );
    let engineering: Vec<i64> = result
        .iter()
        .filter(|r| {
            let n = r["name"].as_str().unwrap();
            n == "Bob" || n == "Alice"
        })
        .map(|r| r["running"].as_i64().unwrap())
        .collect();
    // Ordered by salary: Bob(90000) then Alice(100000).
    assert_eq!(engineering, vec![90_000, 190_000]);
}

#[test]
fn group_by_expression() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT UPPER(department) AS d, COUNT(*) AS n FROM person \
         GROUP BY UPPER(department) ORDER BY d",
    );
    let rows: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            (
                r["d"].as_str().unwrap().to_string(),
                r["n"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("ENGINEERING".to_string(), 2),
            ("RESEARCH".to_string(), 2),
            ("SALES".to_string(), 2),
        ]
    );
}

#[test]
fn correlated_scalar_subquery_counts_out_degree() {
    let f = fixture();
    let result = rows(
        &f.graph,
        "SELECT p.name, \
                (SELECT COUNT(*) FROM rushdb_edges.knows e WHERE e.__source_id = p.__id) AS out_degree \
         FROM person p ORDER BY p.name",
    );
    let degrees: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            (
                r["name"].as_str().unwrap().to_string(),
                r["out_degree"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        degrees,
        vec![
            ("Alice".to_string(), 2),
            ("Bob".to_string(), 1),
            ("Carol".to_string(), 1),
            ("Dave".to_string(), 0),
            ("Eve".to_string(), 0),
            ("Frank".to_string(), 0),
        ]
    );
}

#[test]
fn count_distinct_through_join() {
    let f = fixture();
    let result = one(
        &f.graph,
        "SELECT COUNT(DISTINCT c.name) AS companies \
         FROM person p JOIN rushdb_edges.works_at w ON p.__id = w.__source_id \
         JOIN company c ON c.__id = w.__target_id",
