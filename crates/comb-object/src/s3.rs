use crate::backend::{object_too_large, ObjectBackend, ObjectInfo, Version};
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use comb_core::error::{CoreError, Result};
use std::num::NonZeroU64;
use tokio::io::AsyncReadExt;

/// S3 backend (spec §7.10). Conditional semantics use S3 conditional
/// writes: `If-None-Match: *` for create-only, `If-Match: <etag>` for
/// conditional replacement. The ETag is the provider version token.
pub struct S3Backend {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Backend {
    /// `endpoint` selects an S3-compatible server (e.g. MinIO); path-style
    /// addressing is forced there because virtual-host style needs DNS.
    pub async fn connect(
        profile: Option<&str>,
        region: Option<&str>,
        bucket: &str,
        prefix: &str,
        endpoint: Option<&str>,
    ) -> Self {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(p) = profile {
            loader = loader.profile_name(p);
        }
        if let Some(r) = region {
            loader = loader.region(aws_config::Region::new(r.to_string()));
        }
        let conf = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&conf);
        if let Some(url) = endpoint {
            builder = builder.endpoint_url(url).force_path_style(true);
        }
        S3Backend {
            client: Client::from_conf(builder.build()),
            bucket: bucket.to_string(),
            prefix: prefix.trim_matches('/').to_string(),
        }
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.prefix)
        }
    }

    fn map_put_err<E: ProvideErrorMetadata + std::fmt::Debug>(
        key: &str,
        err: &aws_sdk_s3::error::SdkError<E>,
        creating: bool,
    ) -> CoreError {
        let status = err.raw_response().map(|r| r.status().as_u16());
        let code = err.as_service_error().and_then(|s| s.code()).unwrap_or("");
        match (status, code) {
            (Some(412), _) | (_, "PreconditionFailed") => {
                if creating {
                    CoreError::AlreadyExists(key.into())
                } else {
                    CoreError::PreconditionFailed(format!("{key}: version is stale"))
                }
            }
            (Some(404), _) | (_, "NoSuchKey") => {
                CoreError::PreconditionFailed(format!("{key}: gone"))
            }
            _ => CoreError::BackendUnavailable(format!("s3 put {key}: {err:?}")),
        }
    }

    fn map_get_err<E: ProvideErrorMetadata + std::fmt::Debug>(
        key: &str,
        err: &aws_sdk_s3::error::SdkError<E>,
    ) -> CoreError {
        let status = err.raw_response().map(|r| r.status().as_u16());
        let code = err.as_service_error().and_then(|s| s.code()).unwrap_or("");
        if status == Some(404) || code == "NoSuchKey" {
            CoreError::NotFound(key.into())
        } else {
            CoreError::BackendUnavailable(format!("s3 get {key}: {err:?}"))
        }
    }
}

#[async_trait]
impl ObjectBackend for S3Backend {
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version> {
        self.put_update(key, None, body).await
    }

    async fn put_update(
        &self,
        key: &str,
        expected: Option<&Version>,
        body: &[u8],
    ) -> Result<Version> {
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .body(ByteStream::from(body.to_vec()));
        req = match expected {
            None => req.if_none_match("*"),
            Some(v) => req.if_match(&v.0),
        };
        let out = req
            .send()
            .await
            .map_err(|e| Self::map_put_err(key, &e, expected.is_none()))?;
        Ok(Version(out.e_tag().unwrap_or_default().to_string()))
    }

    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)> {
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
            .map_err(|e| Self::map_get_err(key, &e))?;
        let etag = out.e_tag().unwrap_or_default().to_string();
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| CoreError::BackendUnavailable(format!("s3 body {key}: {e}")))?
            .into_bytes()
            .to_vec();
        Ok((bytes, Version(etag)))
    }

    async fn get_limited(
        &self,
        key: &str,
        max_encoded_bytes: NonZeroU64,
    ) -> Result<(Vec<u8>, Version)> {
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
            .map_err(|e| Self::map_get_err(key, &e))?;
        let etag = out.e_tag().unwrap_or_default().to_string();
        if let Some(n) = out.content_length().and_then(|n| u64::try_from(n).ok()) {
            if n > max_encoded_bytes.get() {
                return Err(object_too_large(key, max_encoded_bytes, Some(n)));
            }
        }
        let take_n = max_encoded_bytes.get().saturating_add(1);
        let mut reader = out.body.into_async_read().take(take_n);
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| CoreError::BackendUnavailable(format!("s3 body {key}: {e}")))?;
        if buf.len() as u64 > max_encoded_bytes.get() {
            return Err(object_too_large(key, max_encoded_bytes, None));
        }
        Ok((buf, Version(etag)))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let status = e.raw_response().map(|r| r.status().as_u16());
                if status == Some(404) {
                    Ok(false)
                } else {
                    Err(CoreError::BackendUnavailable(format!(
                        "s3 head {key}: {e:?}"
                    )))
                }
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
            .map_err(|e| CoreError::BackendUnavailable(format!("s3 delete {key}: {e:?}")))?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let full_prefix = self.full_key(prefix);
        let strip = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);
            if let Some(t) = &token {
                req = req.continuation_token(t);
            }
            let page = req
                .send()
                .await
                .map_err(|e| CoreError::BackendUnavailable(format!("s3 list {prefix}: {e:?}")))?;
            for obj in page.contents() {
                let Some(key) = obj.key() else { continue };
                let key = key.strip_prefix(&strip).unwrap_or(key).to_string();
                let modified = obj
                    .last_modified()
                    .and_then(|t| chrono::DateTime::from_timestamp(t.secs(), 0))
                    .unwrap_or_else(chrono::Utc::now);
                out.push(ObjectInfo { key, modified });
            }
            match page.next_continuation_token() {
                Some(t) => token = Some(t.to_string()),
                None => break,
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn conditional_put_wire_contract() {
        for mode in 0..3 {
            let creating = mode != 2;
            for status in [200, 412, 404, 500] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = format!("http://{}", listener.local_addr().unwrap());
                let server = tokio::spawn(async move {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut raw = Vec::new();
                    let (headers, body) = loop {
                        let mut buf = [0; 1024];
                        let n = stream.read(&mut buf).await.unwrap();
                        assert!(n > 0, "request ended prematurely");
                        raw.extend_from_slice(&buf[..n]);
                        assert!(raw.len() < 16 * 1024);
                        if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8(raw[..end].to_vec()).unwrap();
                            let len: usize = headers
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .map(str::parse)
                                })
                                .unwrap()
                                .unwrap();
                            if raw.len() >= end + 4 + len {
                                break (headers, raw[end + 4..end + 4 + len].to_vec());
                            }
                        }
                    };
                    let response = format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nETag: \"wire-version\"\r\nConnection: close\r\n\r\n");
                    stream.write_all(response.as_bytes()).await.unwrap();
                    (headers, body)
                });
                let config = aws_sdk_s3::config::Builder::new()
                    .behavior_version(BehaviorVersion::latest())
                    .region(Region::new("us-east-1"))
                    .credentials_provider(Credentials::new("test", "test", None, None, "test"))
                    .endpoint_url(endpoint)
                    .force_path_style(true)
                    .retry_config(
                        aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1),
                    )
                    .build();
                let backend = S3Backend {
                    client: Client::from_conf(config),
                    bucket: "bucket".into(),
                    prefix: "prefix".into(),
                };
                let payload = b"\x00payload\xff";
                let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    if mode == 0 {
                        backend.put_create("nested/key", payload).await
                    } else if mode == 1 {
                        backend.put_update("nested/key", None, payload).await
                    } else {
                        backend
                            .put_update("nested/key", Some(&Version("old-version".into())), payload)
                            .await
                    }
                })
                .await
                .unwrap();
                let (headers, body) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), server)
                        .await
                        .unwrap()
                        .unwrap();
                assert!(
                    headers.starts_with("PUT /bucket/prefix/nested/key?x-id=PutObject "),
                    "{}",
                    headers.lines().next().unwrap()
                );
                let lower = headers.to_ascii_lowercase();
                if creating {
                    assert!(lower.contains("\r\nif-none-match: *"));
                    assert!(!lower.contains("\r\nif-match:"));
                } else {
                    assert!(lower.contains("\r\nif-match: old-version"));
                    assert!(!lower.contains("\r\nif-none-match:"));
                }
                assert_eq!(body, payload);
                match status {
                    200 => assert_eq!(result.unwrap().0, "\"wire-version\""),
                    412 if creating => assert!(matches!(result, Err(CoreError::AlreadyExists(_)))),
                    412 | 404 => assert!(matches!(result, Err(CoreError::PreconditionFailed(_)))),
                    500 => assert!(matches!(result, Err(CoreError::BackendUnavailable(_)))),
                    _ => unreachable!(),
                }
            }
        }
    }
}
