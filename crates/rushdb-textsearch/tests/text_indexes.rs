//! Text indexes: declared on the index catalog, maintained by the
//! applier from pending rows, `eventual` (reports lag) or `immediate`
//! (read-your-writes), never pinning the WAL.

use std::collections::HashSet;
use std::sync::Arc;

use rushdb_lmpg::index::{pending, IndexStep};
use rushdb_lmpg::plan::FieldChange;
use rushdb_lmpg::Lmpg;
use rushdb_storage::Store;
use rushdb_textsearch::{LexicalOptions, TextIndexes};
use rushdb_types::ids::TenantId;
use rushdb_types::index::{IndexKind, TextConsistency};
use serde_json::json;

const TENANT: TenantId = TenantId::from_ordinal(21);

fn setup() -> (Lmpg, TextIndexes) {
    let store = Arc::new(Store::new_in_memory());
    (
        Lmpg::new(Arc::clone(&store), TENANT),
        TextIndexes::new(store),
    )
}

fn define(
    lmpg: &Lmpg,
    name: &str,
    label: &str,
    property: &str,
    consistency: TextConsistency,
) -> u32 {
    let def = lmpg
        .define_index(name, label, property, IndexKind::Text { consistency })
        .unwrap();
    while lmpg.index_step(def.id, 1_000).unwrap() != IndexStep::Finished {}
    def.id
}

fn search(lmpg: &Lmpg, text: &TextIndexes, name: &str, query: &str) -> (Vec<u64>, u64) {
    search_opts(lmpg, text, name, query, LexicalOptions::default(), None)
}

fn search_opts(
    lmpg: &Lmpg,
    text: &TextIndexes,
    name: &str,
    query: &str,
    opts: LexicalOptions,
    candidates: Option<Arc<HashSet<u64>>>,
) -> (Vec<u64>, u64) {
    let view = lmpg.store.read_view().unwrap();
    let hits = text
        .search(
            lmpg.tenant,
            &view,
            rushdb_textsearch::TextQuery {
                index: name,
                query,
                opts,
                top_k: 10,
            },
            candidates,
        )
        .unwrap();
    (
        hits.hits.iter().map(|h| h.owner_ord).collect(),
        hits.pending,
    )
}

fn article(lmpg: &Lmpg, title: &str, body: &str) -> rushdb_lmpg::RecordRef {
    lmpg.create_record(
        "Article",
        None,
        vec![
            ("title".into(), FieldChange::Set(json!(title))),
            ("body".into(), FieldChange::Set(json!(body))),
        ],
    )
    .unwrap()
}

#[test]
fn bm25_ranks_relevant_documents_across_all_string_properties() {
    let (lmpg, text) = setup();
    let graph = article(
        &lmpg,
        "Graph database fundamentals",
        "A property graph database stores nodes",
    );
    article(&lmpg, "Cooking pasta", "Boil water and add salt");
    let engine = article(
        &lmpg,
        "Database internals",
        "Storage engines in a database engine",
    );
    define(
        &lmpg,
        "articles",
        "Article",
        "*",
        TextConsistency::Immediate,
    );
    let (hits, pending) = search(&lmpg, &text, "articles", "database");
    assert_eq!(pending, 0);
    assert_eq!(hits.len(), 2);
    assert!(hits.contains(&graph.ordinal) && hits.contains(&engine.ordinal));
    let view = lmpg.store.read_view().unwrap();
    let all = text
        .search(
            lmpg.tenant,
            &view,
            rushdb_textsearch::TextQuery {
                index: "articles",
                query: "database engine",
                opts: LexicalOptions {
                    all_terms: true,
                    ..LexicalOptions::default()
                },
                top_k: 10,
            },
            None,
        )
        .unwrap();
    assert_eq!(
        all.hits.iter().map(|h| h.owner_ord).collect::<Vec<_>>(),
        vec![engine.ordinal]
    );
    // Candidates restrict ranking (hybrid search's filter-before-rank).
    let (scoped, _) = search_opts(
        &lmpg,
        &text,
        "articles",
        "database",
        LexicalOptions::default(),
        Some(Arc::new(HashSet::from([engine.ordinal]))),
    );
    assert_eq!(scoped, vec![engine.ordinal]);
}

#[test]
fn a_property_index_sees_only_that_property() {
    let (lmpg, text) = setup();
    let a = article(&lmpg, "graph", "pasta");
    article(&lmpg, "pasta", "graph");
    define(
        &lmpg,
        "titles",
        "Article",
        "title",
        TextConsistency::Immediate,
    );
    assert_eq!(search(&lmpg, &text, "titles", "graph").0, vec![a.ordinal]);
}

#[test]
fn incremental_index_tracks_insert_update_delete_and_retype() {
    let (lmpg, text) = setup();
    define(
        &lmpg,
        "articles",
        "Article",
        "*",
        TextConsistency::Immediate,
    );
    let a = article(&lmpg, "alpha", "one");
    let b = article(&lmpg, "beta", "two");
    assert_eq!(search(&lmpg, &text, "articles", "alpha").0, vec![a.ordinal]);
    lmpg.update_fields(
        b,
        vec![("title".into(), FieldChange::Set(json!("alpha again")))],
    )
    .unwrap();
    let (mut both, _) = search(&lmpg, &text, "articles", "alpha");
    both.sort_unstable();
    assert_eq!(both, vec![a.ordinal, b.ordinal]);
    lmpg.delete_record(a).unwrap();
    assert_eq!(search(&lmpg, &text, "articles", "alpha").0, vec![b.ordinal]);
    // Leaving the label leaves the index.
    lmpg.retype_record(b, "Article", "Draft").unwrap();
    assert!(search(&lmpg, &text, "articles", "alpha").0.is_empty());
}
