//! Minimal S3 client (SigV4): GET, PUT, multipart upload. Works with AWS S3, MinIO, Cloudflare R2
//! and Google Cloud Storage's S3-interoperable endpoint. Credentials from the environment:
//! AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (optional), AWS_REGION / AWS_DEFAULT_REGION,
//! AWS_ENDPOINT_URL (optional, for non-AWS endpoints). For GCS: GCS_ACCESS_KEY_ID / GCS_SECRET_ACCESS_KEY (HMAC keys).
use super::{env, http_agent};
use anyhow::{bail, Context, Result};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

type HmacSha256 = Hmac<Sha256>;

const PART_SIZE: u64 = 64 * 1024 * 1024; // 64 MiB parts -> up to 640 GB per object with 10k parts
const MULTIPART_THRESHOLD: u64 = 128 * 1024 * 1024;

pub struct Client {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    region: String,
    endpoint: String, // e.g. https://s3.eu-west-1.amazonaws.com
    path_style: bool,
    agent: ureq::Agent,
}

impl Client {
    pub fn from_env(gcs: bool) -> Result<Client> {
        let (ak, sk) = if gcs {
            (env("GCS_ACCESS_KEY_ID").or_else(|| env("AWS_ACCESS_KEY_ID")), env("GCS_SECRET_ACCESS_KEY").or_else(|| env("AWS_SECRET_ACCESS_KEY")))
        } else {
            (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY"))
        };
        let access_key = ak.context("AWS_ACCESS_KEY_ID (or GCS_ACCESS_KEY_ID) not set")?;
        let secret_key = sk.context("AWS_SECRET_ACCESS_KEY (or GCS_SECRET_ACCESS_KEY) not set")?;
        let region = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION")).unwrap_or_else(|| if gcs { "auto".into() } else { "us-east-1".into() });
        let endpoint = if gcs {
            env("GCS_ENDPOINT_URL").unwrap_or_else(|| "https://storage.googleapis.com".into())
        } else {
            env("AWS_ENDPOINT_URL").or_else(|| env("AWS_ENDPOINT_URL_S3")).unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"))
        };
        let path_style = gcs || env("AWS_S3_PATH_STYLE").map(|v| v == "1" || v == "true").unwrap_or(!endpoint.contains("amazonaws.com"));
        Ok(Client { access_key, secret_key, session_token: env("AWS_SESSION_TOKEN"), region, endpoint: endpoint.trim_end_matches('/').into(), path_style, agent: http_agent() })
    }

    fn url(&self, bucket: &str, key: &str, query: &str) -> (String, String, String) {
        // returns (full_url, host, canonical_uri)
        let enc_key = uri_encode(key, false);
        let (host, uri) = if self.path_style {
            let host = self.endpoint.split("://").nth(1).unwrap_or(&self.endpoint).to_string();
            (host, if key.is_empty() { format!("/{bucket}") } else { format!("/{bucket}/{enc_key}") })
        } else {
            let (_scheme, h) = self.endpoint.split_once("://").unwrap_or(("https", &self.endpoint));
            (format!("{bucket}.{h}"), format!("/{enc_key}"))
        };
        let scheme = if self.endpoint.starts_with("http://") { "http" } else { "https" };
        let q = if query.is_empty() { String::new() } else { format!("?{query}") };
        (format!("{scheme}://{host}{uri}{q}"), host, uri)
    }

    fn sign(&self, method: &str, host: &str, uri: &str, query: &str, payload_hash: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
        self.sign_at(chrono::Utc::now(), method, host, uri, query, payload_hash, extra)
    }

    pub(crate) fn sign_at(&self, now: chrono::DateTime<chrono::Utc>, method: &str, host: &str, uri: &str, query: &str, payload_hash: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let mut headers: Vec<(String, String)> = vec![("host".into(), host.into()), ("x-amz-content-sha256".into(), payload_hash.into()), ("x-amz-date".into(), amz_date.clone())];
        if let Some(t) = &self.session_token { headers.push(("x-amz-security-token".into(), t.clone())); }
        for (k, v) in extra { headers.push((k.to_lowercase(), v.to_string())); }
        headers.sort();
        let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{}\n", v.trim())).collect();
        let signed_headers: String = headers.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
        let canonical_query = canonical_query(query);
        let canonical_request = format!("{method}\n{uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", hex::encode(Sha256::digest(canonical_request.as_bytes())));
        let k_date = hmac(format!("AWS4{}", self.secret_key).as_bytes(), date.as_bytes());
        let k_region = hmac(&k_date, self.region.as_bytes());
        let k_service = hmac(&k_region, b"s3");
        let k_signing = hmac(&k_service, b"aws4_request");
        let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));
        let auth = format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}", self.access_key);
        headers.retain(|(k, _)| k != "host");
        headers.push(("authorization".into(), auth));
        headers
    }

    fn request(&self, method: &str, bucket: &str, key: &str, query: &str, body: &[u8], extra: &[(&str, &str)]) -> Result<(u16, Vec<(String, String)>, Vec<u8>)> {
        let (url, host, uri) = self.url(bucket, key, query);
        let payload_hash = hex::encode(Sha256::digest(body));
        let headers = self.sign(method, &host, &uri, query, &payload_hash, extra);
        let mut req = match method {
            "GET" => self.agent.get(&url).force_send_body(),
            "PUT" => self.agent.put(&url),
            "POST" => self.agent.post(&url),
            "DELETE" => self.agent.delete(&url).force_send_body(),
            "HEAD" => self.agent.head(&url).force_send_body(),
            _ => bail!("method {method}"),
        };
        for (k, v) in &headers { req = req.header(k, v); }
        let resp = req.send(body);
        let resp = match resp { Ok(r) => r, Err(ureq::Error::StatusCode(code)) => return Ok((code, vec![], vec![])), Err(e) => return Err(e).with_context(|| format!("S3 {method} {url}")) };
        let status = resp.status().as_u16();
        let mut hdrs = vec![];
        for name in resp.headers().keys() { if let Some(v) = resp.headers().get(name) { hdrs.push((name.as_str().to_lowercase(), v.to_str().unwrap_or("").to_string())); } }
        let mut out = vec![];
        resp.into_body().into_reader().read_to_end(&mut out)?;
        Ok((status, hdrs, out))
    }

    /// Create the bucket if missing. Only used when D2M_S3_CREATE_BUCKET=1 (never on AWS by accident).
    pub fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        if env("D2M_S3_CREATE_BUCKET").as_deref() != Some("1") { return Ok(()); }
        let (st, _, b) = self.request("PUT", bucket, "", "", &[], &[])?;
        if (200..300).contains(&st) || st == 409 { return Ok(()); }
        bail!("S3 create bucket failed: {st} {}", String::from_utf8_lossy(&b))
    }

    pub fn put_object(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        let (st, _, b) = self.request("PUT", bucket, key, "", body, &[])?;
        if !(200..300).contains(&st) { bail!("S3 PUT failed: {st} {}", String::from_utf8_lossy(&b)); }
        Ok(())
    }

    pub fn upload_file(&self, bucket: &str, key: &str, path: &Path, progress: &mut dyn FnMut(u64, u64)) -> Result<()> {
        self.ensure_bucket(bucket)?;
        let size = std::fs::metadata(path)?.len();
        if size < MULTIPART_THRESHOLD {
            let data = std::fs::read(path)?;
            self.put_object(bucket, key, &data)?;
            progress(size, size);
            return Ok(());
        }
        // multipart
        let (st, _, b) = self.request("POST", bucket, key, "uploads=", &[], &[])?;
        if !(200..300).contains(&st) { bail!("S3 CreateMultipartUpload failed: {st} {}", String::from_utf8_lossy(&b)); }
        let xml = String::from_utf8_lossy(&b);
        let upload_id = between(&xml, "<UploadId>", "</UploadId>").context("no UploadId")?.to_string();
        let mut f = std::fs::File::open(path)?;
        let mut etags: Vec<(u32, String)> = vec![];
        let mut sent = 0u64;
        let mut part = 1u32;
        let mut buf = vec![0u8; PART_SIZE as usize];
        while sent < size {
            let n = ((size - sent).min(PART_SIZE)) as usize;
            f.seek(SeekFrom::Start(sent))?;
            f.read_exact(&mut buf[..n])?;
            let q = format!("partNumber={part}&uploadId={}", uri_encode(&upload_id, true));
            let (st, hdrs, b) = self.request("PUT", bucket, key, &q, &buf[..n], &[])?;
            if !(200..300).contains(&st) {
                let _ = self.request("DELETE", bucket, key, &format!("uploadId={}", uri_encode(&upload_id, true)), &[], &[]);
                bail!("S3 UploadPart {part} failed: {st} {}", String::from_utf8_lossy(&b));
            }
            let etag = hdrs.iter().find(|(k, _)| k == "etag").map(|(_, v)| v.clone()).context("no ETag")?;
            etags.push((part, etag));
            sent += n as u64;
            part += 1;
            progress(sent, size);
        }
        let mut body = String::from("<CompleteMultipartUpload>");
        for (p, e) in &etags { body.push_str(&format!("<Part><PartNumber>{p}</PartNumber><ETag>{e}</ETag></Part>")); }
        body.push_str("</CompleteMultipartUpload>");
        let (st, _, b) = self.request("POST", bucket, key, &format!("uploadId={}", uri_encode(&upload_id, true)), body.as_bytes(), &[("content-type", "application/xml")])?;
        if !(200..300).contains(&st) || String::from_utf8_lossy(&b).contains("<Error>") { bail!("S3 CompleteMultipartUpload failed: {st} {}", String::from_utf8_lossy(&b)); }
        Ok(())
    }

    pub fn download_file(&self, bucket: &str, key: &str, target: &Path, progress: &mut dyn FnMut(u64, u64)) -> Result<()> {
        // HEAD for size, then ranged GETs of PART_SIZE to keep memory flat
        let (st, hdrs, _) = self.request("HEAD", bucket, key, "", &[], &[])?;
        if st != 200 { bail!("S3 HEAD {bucket}/{key}: HTTP {st}"); }
        let size: u64 = hdrs.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok()).unwrap_or(0);
        let mut out = std::fs::File::create(target)?;
        let mut got = 0u64;
        loop {
            let end = (got + PART_SIZE - 1).min(size.saturating_sub(1));
            let range = format!("bytes={got}-{end}");
            let (st, _, b) = self.request("GET", bucket, key, "", &[], &[("range", &range)])?;
            if !(200..300).contains(&st) { bail!("S3 GET {bucket}/{key}: HTTP {st}"); }
            out.write_all(&b)?;
            got += b.len() as u64;
            progress(got, size);
            if got >= size || b.is_empty() { break; }
        }
        Ok(())
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).expect("hmac key");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

pub(crate) fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '~' || (c == '/' && !encode_slash) { out.push(c); } else { out.push_str(&format!("%{:02X}", b)); }
    }
    out
}

fn canonical_query(q: &str) -> String {
    if q.is_empty() { return String::new(); }
    let mut parts: Vec<(String, String)> = q.split('&').map(|kv| { let (k, v) = kv.split_once('=').unwrap_or((kv, "")); (uri_encode(&percent_decode(k), true), uri_encode(&percent_decode(v), true)) }).collect();
    parts.sort();
    parts.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s).decode_utf8_lossy().to_string()
}

pub(crate) fn between<'a>(s: &'a str, a: &str, b: &str) -> Option<&'a str> {
    let i = s.find(a)? + a.len();
    let j = s[i..].find(b)? + i;
    Some(&s[i..j])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> Client {
        Client { access_key: "AKIAIOSFODNN7EXAMPLE".into(), secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(), session_token: None,
                 region: "us-east-1".into(), endpoint: "https://s3.amazonaws.com".into(), path_style: false, agent: http_agent() }
    }

    /// AWS "Signature Calculations for the Authorization Header" example: GET /test.txt with Range.
    #[test]
    fn sigv4_known_answer_get_object() {
        let c = client();
        let ts = chrono::DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z").unwrap().with_timezone(&chrono::Utc);
        let empty = hex::encode(Sha256::digest(b""));
        let h = c.sign_at(ts, "GET", "examplebucket.s3.amazonaws.com", "/test.txt", "", &empty, &[("range", "bytes=0-9")]);
        let auth = h.iter().find(|(k, _)| k == "authorization").map(|(_, v)| v.clone()).unwrap();
        assert!(auth.contains("Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"), "{auth}");
        assert!(auth.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date"), "{auth}");
        assert!(auth.ends_with("Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"), "{auth}");
    }

    /// AWS example: PUT /test$file.text with x-amz-storage-class and date header.
    #[test]
    fn sigv4_known_answer_put_object() {
        let c = client();
        let ts = chrono::DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z").unwrap().with_timezone(&chrono::Utc);
        let body_hash = hex::encode(Sha256::digest(b"Welcome to Amazon S3."));
        let h = c.sign_at(ts, "PUT", "examplebucket.s3.amazonaws.com", "/test%24file.text", "", &body_hash,
                          &[("date", "Fri, 24 May 2013 00:00:00 GMT"), ("x-amz-storage-class", "REDUCED_REDUNDANCY")]);
        let auth = h.iter().find(|(k, _)| k == "authorization").map(|(_, v)| v.clone()).unwrap();
        assert!(auth.ends_with("Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"), "{auth}");
    }

    /// AWS example: GET bucket lifecycle (query string signing).
    #[test]
    fn sigv4_known_answer_query() {
        let c = client();
        let ts = chrono::DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z").unwrap().with_timezone(&chrono::Utc);
        let empty = hex::encode(Sha256::digest(b""));
        let h = c.sign_at(ts, "GET", "examplebucket.s3.amazonaws.com", "/", "lifecycle=", &empty, &[]);
        let auth = h.iter().find(|(k, _)| k == "authorization").map(|(_, v)| v.clone()).unwrap();
        assert!(auth.ends_with("Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"), "{auth}");
    }

    #[test]
    fn location_parsing() {
        use crate::storage::Location;
        assert_eq!(Location::parse("s3://b/k/x.bak").unwrap(), Location::S3 { bucket: "b".into(), key: "k/x.bak".into() });
        assert_eq!(Location::parse("az://c/dir/x.bak").unwrap(), Location::Azure { container: "c".into(), blob: "dir/x.bak".into() });
        assert_eq!(Location::parse("https://acct.blob.core.windows.net/c/x.bak").unwrap(), Location::Azure { container: "c".into(), blob: "x.bak".into() });
        assert_eq!(Location::parse("gs://b/x").unwrap(), Location::Gcs { bucket: "b".into(), key: "x".into() });
        assert!(Location::parse("/tmp/x.bak").unwrap().is_local());
        assert_eq!(Location::parse("s3://b/k/x.bak").unwrap().file_name(), "x.bak");
    }
}
