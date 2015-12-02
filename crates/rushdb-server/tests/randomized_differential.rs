//! 2C — randomized property tests: the optimized LMPG query path must agree
//! with a simple O(n) reference evaluator over the plain record set. A seeded
//! PRNG drives record generation and query predicates; any divergence indicates
//! an optimization (index, pushdown, early-exit) changed the documented
//! semantics.

use rushdb_protocol::{
    Mutation, Operation, Query, QueryDocument, RequestContext, RequestEnvelope, Response,
};
use rushdb_server::{Node, ServerConfig};

// ── tiny deterministic PRNG (xorshift64) ──────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the pick index is bounded by the slice length, so the u64-to-usize conversion is lossless on supported targets"
    )]
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

const LABELS: [&str; 3] = ["ALPHA", "BETA", "GAMMA"];
const NAMES: [&str; 6] = ["ada", "bob", "carol", "dave", "erin", "frank"];

fn node() -> Node {
    let mut cfg = ServerConfig::default();
    Node::bootstrap(&mut cfg).unwrap()
}

/// Reference state: label + plain properties per generated record.
struct ReferenceState {
    records: Vec<(String, serde_json::Value)>,
}

fn put(node: &Node, label: &str, props: serde_json::Value) {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("rnd-put".into()),
        context: RequestContext::default(),
        operation: Operation::Mutate {
            mutation: Mutation::PutRecord {
                mode: rushdb_protocol::PutMode::Create,
                id: None,
                label: Some(label.to_string()),
                properties: props,
                vectors: vec![],
                options: serde_json::json!({}),
            },
        },
    };
    rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(envelope)
        .unwrap();
}

fn find_contents(node: &Node, doc: QueryDocument) -> Vec<String> {
    let envelope = RequestEnvelope {
        protocol_version: 1,
        request_id: Some("rnd-find".into()),
        context: RequestContext::default(),
        operation: Operation::Query {
            query: Query::Records { query: doc },
        },
    };
    let res = rushdb_server::protocol::ProtocolExecutor::new(node, None)
        .execute(envelope)
        .unwrap();
    let Response::Records { records, .. } = res.response else {
        panic!("expected records");
    };
    let mut contents = records
        .into_iter()
        .map(|record| {
            format!(
                "{}:{}",
                record.get("label").map_or("null".into(), canonical_json),
                record
                    .get("properties")
                    .map_or("null".into(), canonical_json)
            )
        })
        .collect::<Vec<_>>();
    contents.sort();
    contents
}

/// Naive reference evaluator: label membership + a single scalar predicate.
fn reference_eval(
    state: &ReferenceState,
    labels: &[String],
    predicate: &serde_json::Value,
) -> Vec<(String, serde_json::Value)> {
    state
        .records
        .iter()
        .filter(|(label, _props)| labels.is_empty() || labels.iter().any(|l| l == label))
        .filter(|(_, props)| eval_predicate(props, predicate))
        .cloned()
        .collect()
}

fn eval_predicate(props: &serde_json::Value, predicate: &serde_json::Value) -> bool {
    let Some(obj) = predicate.as_object() else {
        return false;
    };
    // Logical AND of all conditions.
    obj.iter().all(|(field, cond)| {
        let value = props.get(field);
        match cond {
            serde_json::Value::Null => value.is_some_and(serde_json::Value::is_null),
            serde_json::Value::Object(op) => {
                let (op_name, operand) = op.iter().next().unwrap();
                match op_name.as_str() {
                    "$gt" => value
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|v| v > operand.as_i64().unwrap()),
                    "$gte" => value
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|v| v >= operand.as_i64().unwrap()),
                    "$lt" => value
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|v| v < operand.as_i64().unwrap()),
                    "$lte" => value
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|v| v <= operand.as_i64().unwrap()),
