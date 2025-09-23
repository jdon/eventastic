/// Preserve your data so it can be stored in the database.
///
/// [`Pickle`] is implemented for types that implement [`serde::Serialize`]
/// and [`serde::de::DeserializeOwned`]. It uses [`serde_json`] to do the
/// serialization.
pub trait Pickle: Sized {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Convert to bytes for storage.
    fn pickle(&self) -> Result<Vec<u8>, Self::Error>;

    /// Convert back to `Self` from bytes.
    fn unpickle(bytes: &[u8]) -> Result<Self, Self::Error>;
}

#[cfg(feature = "serde")]
impl<T> Pickle for T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    type Error = serde_json::Error;

    fn pickle(&self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(self)
    }

    fn unpickle(bytes: &[u8]) -> Result<Self, Self::Error> {
        serde_json::from_slice(bytes)
    }
}
