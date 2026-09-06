use crate::backend::{ObjectBackend, ObjectInfo, Version};
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use comb_core::error::{CoreError, Result};

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
}

#[async_trait]
impl ObjectBackend for S3Backend {
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version> {
        let out = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .if_none_match("*")
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .map_err(|e| Self::map_put_err(key, &e, true))?;
        Ok(Version(out.e_tag().unwrap_or_default().to_string()))
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
            .map_err(|e| {
                let code = e.as_service_error().and_then(|s| s.code()).unwrap_or("");
                if code == "NoSuchKey" {
                    CoreError::NotFound(key.into())
                } else {
                    CoreError::BackendUnavailable(format!("s3 get {key}: {e:?}"))
                }
            })?;
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
