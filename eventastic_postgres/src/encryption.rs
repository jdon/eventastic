use async_trait::async_trait;

/// Encrypt data before storing it in the database.
#[async_trait]
pub trait EncryptionProvider {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Encrypt a batch of items. The batch size won't exceed the value returned
    /// by [`max_batch_size`].
    async fn encrypt(&self, plain: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Decrypt a batch of items. The batch size won't exceed the value returned
    /// by [`max_batch_size`].
    async fn decrypt(&self, cipher: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// The maximum batch size to use for [`encrypt`] and [`decrypt`] operations.
    fn max_batch_size(&self) -> usize;
}

/// An [`EncryptionProvider`] that does no encryption. Can be used where you
/// don't need any encryption.
#[derive(Clone)]
pub struct NoEncryption;

#[async_trait]
impl EncryptionProvider for NoEncryption {
    type Error = NoEncryptionError;

    async fn encrypt(&self, plain: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(plain)
    }

    async fn decrypt(&self, cipher: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(cipher)
    }

    fn max_batch_size(&self) -> usize {
        1
    }
}

/// The error type for [`NoEncryption`].
///
/// This can't actually be returned by `encrypt` or `decrypt` but is required by the trait.
#[derive(Debug)]
pub struct NoEncryptionError;

impl std::fmt::Display for NoEncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoEncryptionError")
    }
}

impl std::error::Error for NoEncryptionError {}
