//! Signed capability tokens (plan document 14/25 security transport): a
//! capability is `base64url(claims).sig` where `sig` is HMAC-SHA256 over the
//! claims with a server-held secret. Claims carry subject, scope (space
//! separated permissions), an optional opaque tenant binding, issuance and
//! expiry. Verification checks the signature and expiry; transports enforce
//! permission and exact tenant binding on every connection/request.
//! An opaque token is never authority on its own — the signature binds it to
//! the issuing secret, and the scope gates every route.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Verified claims carried by a signed capability token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capability {
    /// Subject the capability was issued to.
    pub sub: String,
    /// Space-separated permissions granted to the subject.
    pub scope: String,
    /// Opaque scope value this capability is allowed to select.
    /// `None` is valid only for a single-tenant server.
    pub tenant: Option<String>,
    /// Issued-at time as a Unix timestamp in seconds.
    pub iat: u64,
    /// Expiry time as a Unix timestamp in seconds.
    pub exp: u64,
}

/// Failure modes when issuing or verifying a capability token.
