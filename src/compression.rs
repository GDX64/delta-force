//! [`Compression`]: how a table compresses its data files.
use std::fmt;

use delta_kernel::parquet::basic::{Compression as Codec, GzipLevel, ZstdLevel};

/// The table property that stores a table's [`Compression`].
pub(crate) const PROPERTY: &str = "delta-force.compression";

/// How a table compresses its data files. Set it with
/// [`TableConfig::compression`](crate::TableConfig::compression); it is stored
/// with the table, so every writer uses it. Readers don't need it: each file
/// records its own codec.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Compression {
    None,
    Snappy,
    Gzip,
    Lz4,
    /// Best size for the speed; the default.
    #[default]
    Zstd,
}

impl Compression {
    fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Snappy => "snappy",
            Compression::Gzip => "gzip",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        }
    }

    /// Parse a stored property value. Unknown names are `None`.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        [Self::None, Self::Snappy, Self::Gzip, Self::Lz4, Self::Zstd]
            .into_iter()
            .find(|c| c.name() == name)
    }

    pub(crate) fn property(self) -> (&'static str, &'static str) {
        (PROPERTY, self.name())
    }

    pub(crate) fn codec(self) -> Codec {
        match self {
            Compression::None => Codec::UNCOMPRESSED,
            Compression::Snappy => Codec::SNAPPY,
            Compression::Gzip => Codec::GZIP(GzipLevel::default()),
            Compression::Lz4 => Codec::LZ4_RAW,
            Compression::Zstd => Codec::ZSTD(ZstdLevel::default()),
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}
