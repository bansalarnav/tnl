use std::{error::Error, fmt, str::FromStr};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TunnelId(String);

impl TunnelId {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidTunnelId> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 63
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(InvalidTunnelId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TunnelId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for TunnelId {
    type Err = InvalidTunnelId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for TunnelId {
    type Error = InvalidTunnelId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct InvalidTunnelId;

impl fmt::Display for InvalidTunnelId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "tunnel name must contain only lowercase letters, numbers, and internal hyphens",
        )
    }
}

impl Error for InvalidTunnelId {}

#[cfg(test)]
mod tests {
    use super::TunnelId;

    #[test]
    fn validates_tunnel_ids() {
        for valid in ["a", "my-app", "app123"] {
            assert!(TunnelId::new(valid).is_ok(), "{valid}");
        }
        for invalid in ["", "-app", "app-", "MyApp", "a.b", &"a".repeat(64)] {
            assert!(TunnelId::new(invalid).is_err(), "{invalid}");
        }
    }
}
