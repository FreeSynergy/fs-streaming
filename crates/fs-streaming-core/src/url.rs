//! Target addresses, parsed strictly.
//!
//! Pattern: Newtype with a validating constructor — `rtmp` or `rtmps`, a host, an optional port and an
//! application path. No user, no query, no fragment: those are where keys leak. A key hidden inside
//! the path cannot be told apart; the interface asks for the key separately.

use std::fmt;

use crate::error::{Error, Result};

/// Why an address was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UrlProblem {
    /// Not `rtmp://` or `rtmps://`.
    #[error("the address must start with rtmp:// or rtmps://")]
    Scheme,
    /// No host or a host with odd characters.
    #[error("the address needs a plain host name")]
    Host,
    /// A port that is not a number from 1 to 65535.
    #[error("the port is not valid")]
    Port,
    /// No application path.
    #[error("the address needs an application path")]
    Path,
    /// A user name or password in the address.
    #[error("the address must not contain a user or password")]
    User,
    /// A query or a fragment.
    #[error("the address must not contain a query or a fragment")]
    QueryOrFragment,
    /// Longer than 2 KiB.
    #[error("the address is too long")]
    TooLong,
}

/// A checked target address.
#[derive(Clone, PartialEq, Eq)]
pub struct TargetUrl {
    secure: bool,
    host: String,
    port: Option<u16>,
    path: String,
}

impl TargetUrl {
    /// Parse and check.
    pub fn parse(text: &str) -> Result<Self> {
        Self::parse_inner(text).map_err(Error::InvalidUrl)
    }

    fn parse_inner(text: &str) -> std::result::Result<Self, UrlProblem> {
        if text.len() > 2048 {
            return Err(UrlProblem::TooLong);
        }
        if text
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || c == '\\')
        {
            return Err(UrlProblem::Host);
        }
        let lower = text.to_ascii_lowercase();
        let (secure, rest) = if lower.starts_with("rtmps://") {
            (true, &text[8..])
        } else if lower.starts_with("rtmp://") {
            (false, &text[7..])
        } else {
            return Err(UrlProblem::Scheme);
        };
        if rest.contains('?') || rest.contains('#') {
            return Err(UrlProblem::QueryOrFragment);
        }
        let (authority, path) = rest.split_once('/').ok_or(UrlProblem::Path)?;
        if authority.contains('@') {
            return Err(UrlProblem::User);
        }
        let path = path.trim_matches('/');
        if path.is_empty() || path.contains("//") || path.contains('%') {
            return Err(UrlProblem::Path);
        }
        let (host, port) = split_host_port(authority)?;
        Ok(Self {
            secure,
            host,
            port,
            path: path.to_string(),
        })
    }

    /// `rtmps`.
    pub fn is_secure(&self) -> bool {
        self.secure
    }
    /// Host name, lower case (IPv6 without brackets).
    pub fn host(&self) -> &str {
        &self.host
    }
    /// The port, the default of the scheme if none was given.
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(if self.secure { 443 } else { 1935 })
    }
    /// The application path, no slashes at the ends.
    pub fn app(&self) -> &str {
        &self.path
    }
    /// Whether a key may stay when the address changes from `self` to `new`: same host, the same port
    /// (two addresses without a port count as the same, so going to rtmps keeps it), and not from
    /// rtmps to rtmp.
    pub fn keeps_key_for(&self, new: &Self) -> bool {
        let same_port = self.port == new.port || self.port() == new.port();
        self.host == new.host && same_port && (new.secure || !self.secure)
    }
}

fn split_host_port(authority: &str) -> std::result::Result<(String, Option<u16>), UrlProblem> {
    let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
        let (h, after) = inner.split_once(']').ok_or(UrlProblem::Host)?;
        if h.parse::<std::net::Ipv6Addr>().is_err() {
            return Err(UrlProblem::Host);
        }
        match after {
            "" => (h, None),
            a => (h, Some(a.strip_prefix(':').ok_or(UrlProblem::Host)?)),
        }
    } else {
        match authority.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        }
    };
    let bracketed = authority.starts_with('[');
    let ok = !host.is_empty()
        && host.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '-') || (bracketed && c == ':')
        });
    if !ok {
        return Err(UrlProblem::Host);
    }
    let port = match port {
        None => None,
        Some(p) => Some(
            p.parse::<u16>()
                .ok()
                .filter(|p| *p != 0)
                .ok_or(UrlProblem::Port)?,
        ),
    };
    Ok((host.to_ascii_lowercase(), port))
}

impl fmt::Display for TargetUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scheme = if self.secure { "rtmps" } else { "rtmp" };
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        match self.port {
            Some(p) => write!(f, "{scheme}://{host}:{p}/{}", self.path),
            None => write!(f, "{scheme}://{host}/{}", self.path),
        }
    }
}

impl fmt::Debug for TargetUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TargetUrl({self})")
    }
}
