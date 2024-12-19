use crate::intern::{EndpointIdInt, EndpointIdTag, InternId};

macro_rules! smol_str_wrapper {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(smol_str::SmolStr);

        impl $name {
            #[allow(unused)]
            pub(crate) fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl<T> std::cmp::PartialEq<T> for $name
        where
            smol_str::SmolStr: std::cmp::PartialEq<T>,
        {
            fn eq(&self, other: &T) -> bool {
                self.0.eq(other)
            }
        }

        impl<T> From<T> for $name
        where
            smol_str::SmolStr: From<T>,
        {
            fn from(x: T) -> Self {
                Self(x.into())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                &*self.0
            }
        }

        impl<'de> serde::de::Deserialize<'de> for $name {
            fn deserialize<D: serde::de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                <smol_str::SmolStr as serde::de::Deserialize<'de>>::deserialize(d).map(Self)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(s)
            }
        }
    };
}

const POOLER_SUFFIX: &str = "-pooler";
pub(crate) const LOCAL_PROXY_SUFFIX: &str = "-local-proxy";

impl EndpointId {
    #[must_use]
    fn normalize_str(&self) -> &str {
        if let Some(stripped) = self.as_ref().strip_suffix(POOLER_SUFFIX) {
            stripped
        } else if let Some(stripped) = self.as_ref().strip_suffix(LOCAL_PROXY_SUFFIX) {
            stripped
        } else {
            self
        }
    }

    #[must_use]
    pub fn normalize(&self) -> Self {
        self.normalize_str().into()
    }

    #[must_use]
    pub fn normalize_intern(&self) -> EndpointIdInt {
        EndpointIdTag::get_interner().get_or_intern(self.normalize_str())
    }
}

// 90% of role name strings are 20 characters or less.
smol_str_wrapper!(RoleName);
// 50% of endpoint strings are 23 characters or less.
smol_str_wrapper!(EndpointId);
// 50% of branch strings are 23 characters or less.
smol_str_wrapper!(BranchId);
// 90% of project strings are 23 characters or less.
smol_str_wrapper!(ProjectId);
// 90% of account strings are 23 characters or less.
smol_str_wrapper!(AccountId);

// will usually equal endpoint ID
smol_str_wrapper!(EndpointCacheKey);

smol_str_wrapper!(DbName);

// postgres hostname, will likely be a port:ip addr
smol_str_wrapper!(Host);

// Endpoints are a bit tricky. Rare they might be branches or projects.
impl EndpointId {
    pub(crate) fn is_endpoint(&self) -> bool {
        self.0.starts_with("ep-")
    }
    pub(crate) fn is_branch(&self) -> bool {
        self.0.starts_with("br-")
    }
}
