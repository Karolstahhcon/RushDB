//! Gremlin bytecode (`GraphSON` 3) runs as the equivalent Gremlin traversal:
//! writes through the mutation path, reads through the query path.

use super::*;
use rushdb_protocol::{RequestContext, RequestEnvelope};

fn execute(node: &Node, operation: Operation) -> Result<Response, ProtocolError> {
    ProtocolExecutor::new(node, None)
        .execute(RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: None,
            context: RequestContext::default(),
            operation,
        })
        .map(|envelope| envelope.response)
}

#[test]
fn bytecode_reads_and_writes_like_gremlin_text() {
    let mut cfg = super::tests::config(false);
