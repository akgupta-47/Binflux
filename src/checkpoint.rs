use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub binlog_file: String,
    pub binlog_position: u32,
    pub gtid: Option<String>,
    /// Full executed GTID set used for GTID-based resume. Older checkpoints omit this field.
    #[serde(default)]
    pub gtid_set: Option<String>,
}

pub fn load(path: &Path) -> Result<Option<Checkpoint>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).with_context(|| format!("reading checkpoint {}", path.display()))?;
    let checkpoint = serde_json::from_slice(&bytes)
        .with_context(|| format!("decoding checkpoint {}", path.display()))?;
    Ok(Some(checkpoint))
}

pub fn persist(path: &Path, checkpoint: &Checkpoint) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("creating checkpoint directory {}", parent.display()))?;
    let tmp = temp_path(path);
    let bytes = serde_json::to_vec(checkpoint)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .with_context(|| format!("creating checkpoint temp file {}", tmp.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)
            .with_context(|| format!("replacing checkpoint {}", path.display()))?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

pub fn reset(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)?.sync_all()?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing checkpoint {}", path.display())),
    }
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    name.push(format!(".{}.{}.tmp", std::process::id(), nonce));
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_replaces_and_loads_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let cp = Checkpoint {
            binlog_file: "mysql-bin.000001".into(),
            binlog_position: 123,
            gtid: None,
            gtid_set: Some("24bc7850-2c16-11e6-a073-0242ac110002:1-3".into()),
        };
        persist(&path, &cp).unwrap();
        assert_eq!(load(&path).unwrap(), Some(cp));
        reset(&path).unwrap();
        assert_eq!(load(&path).unwrap(), None);
    }

    #[test]
    fn loads_checkpoints_written_before_gtid_set_support() {
        let old = br#"{"binlog_file":"mysql-bin.000001","binlog_position":123,"gtid":"uuid:1"}"#;
        let checkpoint: Checkpoint = serde_json::from_slice(old).unwrap();
        assert_eq!(checkpoint.gtid.as_deref(), Some("uuid:1"));
        assert_eq!(checkpoint.gtid_set, None);
    }
}
