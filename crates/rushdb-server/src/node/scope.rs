//! Explicit per-request tenant scope.
//!
//! Replaces the former ambient thread-local: the scope is now a value
//! constructed once at the transport boundary, from the `scope: Option<String>`
//! the transport already authenticated (HTTP header, gRPC/Bolt metadata,
//! pgwire startup parameter — all resolve this explicitly before this type
//! ever exists), and threaded through `ProtocolExecutor` into every `Node`
//! method that needs it. No code path can read a tenant identity that
//! "happens to still be set" from an earlier, unrelated call — the value is
//! either in hand as a parameter or it doesn't exist.

use rushdb_types::ids::TenantId;

use super::Node;

/// Request-scoped view over a booted [`Node`]. `self.tenant()` resolves from
/// the value carried here, never from ambient state. `Deref`s to `Node` so
/// every method that doesn't need the tenant stays callable unchanged;
/// methods that do need it are defined on `RequestScope` instead of `Node`
/// directly, so they cannot compile without one.
///
/// `pub`, not `pub(crate)`: this is the replacement for the deleted
/// `Node::set_request_scope` as a calling convention, including for external
/// callers (tests, embedders) that need to act as a specific tenant — "set
/// the ambient scope, then call the method" becomes "construct a
/// `RequestScope` for that tenant, then call the method on it." Fields stay
/// private; a caller cannot build one except through [`Self::new`]/
/// [`Self::for_tenant`], so the tenant is always resolved the one documented
/// way.
#[derive(Clone)]
pub struct RequestScope<'a> {
    node: &'a Node,
