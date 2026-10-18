//! Minimal Azure Blob Storage client: block blob upload (staged blocks) and ranged download.
//! Auth: AZURE_STORAGE_SAS_TOKEN (preferred), or AZURE_STORAGE_KEY (Shared Key), with AZURE_STORAGE_ACCOUNT,
//! or AZURE_STORAGE_CONNECTION_STRING.
use super::{env, http_agent};
use anyhow::{bail, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const BLOCK_SIZE: u64 = 100 * 1024 * 1024; // 100 MiB blocks; 50k blocks max -> ~4.75 TB
const SINGLE_PUT_MAX: u64 = 256 * 1024 * 1024;

pub struct Client {
    account: String,
    key: Option<Vec<u8>>,
    sas: Option<String>,
    endpoint: String,
    agent: ureq::Agent,
}

impl Client {
    pub fn from_env() -> Result<Client> {
        let mut account = env("AZURE_STORAGE_ACCOUNT");
        let mut key = env("AZURE_STORAGE_KEY");
        let mut sas = env("AZURE_STORAGE_SAS_TOKEN");
        let mut endpoint = env("AZURE_STORAGE_ENDPOINT");
        if let Some(cs) = env("AZURE_STORAGE_CONNECTION_STRING") {
            for part in cs.split(';') {
                if let Some((k, v)) = part.split_once('=') {
                    match k { "AccountName" => account = Some(v.into()), "AccountKey" => key = Some(part[k.len() + 1..].into()), "SharedAccessSignature" => sas = Some(part[k.len() + 1..].into()), "BlobEndpoint" => endpoint = Some(v.into()), _ => {} }
                }
            }
        }
        let account = account.context("AZURE_STORAGE_ACCOUNT not set")?;
        if key.is_none() && sas.is_none() { bail!("set AZURE_STORAGE_SAS_TOKEN or AZURE_STORAGE_KEY"); }
        let key = key.map(|k| base64::engine::general_purpose::STANDARD.decode(k.trim())).transpose().context("AZURE_STORAGE_KEY is not base64")?;
        let endpoint = endpoint.unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
        Ok(Client { account, key, sas: sas.map(|s| s.trim_start_matches('?').to_string()), endpoint: endpoint.trim_end_matches('/').into(), agent: http_agent() })
    }

    fn url(&self, container: &str, blob: &str, query: &str) -> String {
        let mut q = query.to_string();
        if let Some(s) = &self.sas { if !q.is_empty() { q.push('&'); } q.push_str(s); }
        let sep = if q.is_empty() { "" } else { "?" };
        if blob.is_empty() { return format!("{}/{container}{sep}{q}", self.endpoint); }
        format!("{}/{container}/{}{sep}{q}", self.endpoint, super::s3::uri_encode(blob, false))
    }

    fn request(&self, method: &str, container: &str, blob: &str, query: &str, body: &[u8], extra: &[(&str, &str)]) -> Result<(u16, Vec<(String, String)>, Vec<u8>)> {
        let url = self.url(container, blob, query);
        let date = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let mut headers: Vec<(String, String)> = vec![("x-ms-date".into(), date), ("x-ms-version".into(), "2021-08-06".into())];
        for (k, v) in extra { headers.push((k.to_string(), v.to_string())); }
        if let Some(key) = &self.key {
            // Shared Key authorization
            let mut ms: Vec<(String, String)> = headers.iter().filter(|(k, _)| k.starts_with("x-ms-")).map(|(k, v)| (k.to_lowercase(), v.clone())).collect();
            ms.sort();
            let canon_headers: String = ms.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
            let ep_path = self.endpoint.split("://").nth(1).and_then(|h| h.find('/').map(|i| h[i..].to_string())).unwrap_or_default();
            let path = if blob.is_empty() { format!("{ep_path}/{container}") } else { format!("{ep_path}/{container}/{}", super::s3::uri_encode(blob, false)) };
            let mut canon_res = format!("/{}{}", self.account, path);
            let mut qs: Vec<(String, String)> = query.split('&').filter(|p| !p.is_empty()).map(|p| { let (k, v) = p.split_once('=').unwrap_or((p, "")); (k.to_lowercase(), percent_encoding::percent_decode_str(v).decode_utf8_lossy().to_string()) }).collect();
            qs.sort();
            for (k, v) in qs { canon_res.push_str(&format!("\n{k}:{v}")); }
            let content_length = if body.is_empty() { String::new() } else { body.len().to_string() };
            let content_type = extra.iter().find(|(k, _)| k.eq_ignore_ascii_case("content-type")).map(|(_, v)| *v).unwrap_or("");
            let range = extra.iter().find(|(k, _)| k.eq_ignore_ascii_case("range")).map(|(_, v)| *v).unwrap_or("");
            // VERB, Content-Encoding, Content-Language, Content-Length, Content-MD5, Content-Type, Date,
            // If-Modified-Since, If-Match, If-None-Match, If-Unmodified-Since, Range, CanonicalizedHeaders, CanonicalizedResource
            let sts = format!("{method}\n\n\n{content_length}\n\n{content_type}\n\n\n\n\n\n{range}\n{canon_headers}{canon_res}");
            let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("key");
            mac.update(sts.as_bytes());
            let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
            headers.push(("Authorization".into(), format!("SharedKey {}:{sig}", self.account)));
        }
        let mut req = match method {
            "GET" => self.agent.get(&url).force_send_body(),
            "PUT" => self.agent.put(&url),
            "HEAD" => self.agent.head(&url).force_send_body(),
            _ => bail!("method {method}"),
        };
        for (k, v) in &headers { req = req.header(k, v); }
        let resp = match req.send(body) { Ok(r) => r, Err(ureq::Error::StatusCode(code)) => return Ok((code, vec![], vec![])), Err(e) => return Err(e).with_context(|| format!("Azure {method} {container}/{blob}")) };
        let status = resp.status().as_u16();
        let mut hdrs = vec![];
        for name in resp.headers().keys() { if let Some(v) = resp.headers().get(name) { hdrs.push((name.as_str().to_lowercase(), v.to_str().unwrap_or("").to_string())); } }
        let mut out = vec![];
        resp.into_body().into_reader().read_to_end(&mut out)?;
        Ok((status, hdrs, out))
    }

    /// Create the container if it does not exist (idempotent).
    pub fn ensure_container(&self, container: &str) -> Result<()> {
        let (st, _, b) = self.request("PUT", container, "", "restype=container", &[], &[])?;
        if (200..300).contains(&st) || st == 409 { return Ok(()); }
        bail!("Azure create container failed: {st} {}", String::from_utf8_lossy(&b))
    }

    pub fn upload_file(&self, container: &str, blob: &str, path: &Path, progress: &mut dyn FnMut(u64, u64)) -> Result<()> {
        if let Err(e) = self.ensure_container(container) { eprintln!("warning: could not create container {container}: {e}"); }
        let size = std::fs::metadata(path)?.len();
        if size <= SINGLE_PUT_MAX {
            let data = std::fs::read(path)?;
            let (st, _, b) = self.request("PUT", container, blob, "", &data, &[("x-ms-blob-type", "BlockBlob"), ("content-type", "application/octet-stream")])?;
            if !(200..300).contains(&st) { bail!("Azure PutBlob failed: {st} {}", String::from_utf8_lossy(&b)); }
            progress(size, size);
            return Ok(());
        }
        let mut f = std::fs::File::open(path)?;
        let mut ids = vec![];
        let mut sent = 0u64;
        let mut buf = vec![0u8; BLOCK_SIZE as usize];
        let mut i = 0u32;
        while sent < size {
            let n = ((size - sent).min(BLOCK_SIZE)) as usize;
            f.seek(SeekFrom::Start(sent))?;
            f.read_exact(&mut buf[..n])?;
            let id = base64::engine::general_purpose::STANDARD.encode(format!("block-{i:08}"));
            let q = format!("comp=block&blockid={}", super::s3::uri_encode(&id, true));
            let (st, _, b) = self.request("PUT", container, blob, &q, &buf[..n], &[("content-type", "application/octet-stream")])?;
            if !(200..300).contains(&st) { bail!("Azure PutBlock failed: {st} {}", String::from_utf8_lossy(&b)); }
            ids.push(id);
            sent += n as u64;
            i += 1;
            progress(sent, size);
        }
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for id in &ids { xml.push_str(&format!("<Latest>{id}</Latest>")); }
        xml.push_str("</BlockList>");
        let (st, _, b) = self.request("PUT", container, blob, "comp=blocklist", xml.as_bytes(), &[("content-type", "application/xml")])?;
        if !(200..300).contains(&st) { bail!("Azure PutBlockList failed: {st} {}", String::from_utf8_lossy(&b)); }
        Ok(())
    }

    pub fn download_file(&self, container: &str, blob: &str, target: &Path, progress: &mut dyn FnMut(u64, u64)) -> Result<()> {
        let (st, hdrs, _) = self.request("HEAD", container, blob, "", &[], &[])?;
        if st != 200 { bail!("Azure HEAD {container}/{blob}: HTTP {st}"); }
        let size: u64 = hdrs.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok()).unwrap_or(0);
        let mut out = std::fs::File::create(target)?;
        let mut got = 0u64;
        loop {
            let end = (got + BLOCK_SIZE - 1).min(size.saturating_sub(1));
            let range = format!("bytes={got}-{end}");
            let (st, _, b) = self.request("GET", container, blob, "", &[], &[("range", &range)])?;
            if !(200..300).contains(&st) { bail!("Azure GET {container}/{blob}: HTTP {st}"); }
            out.write_all(&b)?;
            got += b.len() as u64;
            progress(got, size);
            if got >= size || b.is_empty() { break; }
        }
        Ok(())
    }
}
