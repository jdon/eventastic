use async_trait::async_trait;
use eventastic_postgres::EncryptionProvider;

/// Doesn't actually encrypt just does one XOR with the series 0, 1, 2..255, 0..
/// to encrypt it then decrypts it by doing the same operation again. (A XOR B)
/// XOR B = A.
#[derive(Clone)]
pub struct TestEncryptionProvider;

#[async_trait]
impl EncryptionProvider for TestEncryptionProvider {
    type Error = TestEncryptionError;

    async fn encrypt(&self, plain: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(plain
            .into_iter()
            .map(|plain| {
                plain
                    .into_iter()
                    .enumerate()
                    .map(|(key, plain)| plain ^ (key as u8))
                    .collect()
            })
            .collect())
    }

    async fn decrypt(&self, cipher: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(cipher
            .into_iter()
            .map(|cipher| {
                cipher
                    .into_iter()
                    .enumerate()
                    .map(|(key, cipher)| cipher ^ (key as u8))
                    .collect()
            })
            .collect())
    }

    fn max_batch_size(&self) -> usize {
        42
    }
}

#[derive(Debug)]
pub struct TestEncryptionError;

impl std::fmt::Display for TestEncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestEncryptionError")
    }
}

impl std::error::Error for TestEncryptionError {}

#[tokio::test]
async fn encryption_changes_the_data() {
    // Arrange
    let encryption_provider = TestEncryptionProvider;
    let plain = b"Hello, World!";

    // Act
    let cipher = encryption_provider
        .encrypt(vec![plain.into()])
        .await
        .unwrap();

    // Assert
    assert_eq!(cipher.len(), 1);
    assert_ne!(&cipher[0], plain);
}

#[tokio::test]
async fn encrypt_then_decrypt_returns_original_data() {
    // Arrange
    let encryption_provider = TestEncryptionProvider;
    let plain = b"Hello, World!";

    // Act
    let cipher = encryption_provider
        .encrypt(vec![plain.into()])
        .await
        .unwrap();
    let decrypted = encryption_provider.decrypt(cipher).await.unwrap();

    // Assert
    assert_eq!(decrypted.len(), 1);
    assert_eq!(&decrypted[0], plain);
}
