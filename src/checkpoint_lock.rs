use anyhow::{Context, Result, bail};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

/// Holds an advisory OS lock for the lifetime of a Binflux run.
///
/// The lock file is intentionally left on disk: the kernel lock, not the file's
/// existence, indicates whether another process currently owns the checkpoint.
pub struct CheckpointLock {
    file: File,
}

impl CheckpointLock {
    pub fn acquire(checkpoint_path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            let path = lock_path(checkpoint_path);
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent).with_context(|| {
                    format!("creating checkpoint lock directory {}", parent.display())
                })?;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .with_context(|| format!("opening checkpoint lock {}", path.display()))?;
            // The OS releases flock locks automatically if the process exits or crashes.
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    bail!(
                        "checkpoint {} is already in use by another Binflux process",
                        checkpoint_path.display()
                    );
                }
                return Err(error)
                    .with_context(|| format!("locking checkpoint {}", checkpoint_path.display()));
            }
            Ok(Self { file })
        }
        #[cfg(not(unix))]
        {
            let _ = checkpoint_path;
            bail!("exclusive checkpoint locking is currently supported on Unix platforms only")
        }
    }
}

impl Drop for CheckpointLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

fn lock_path(checkpoint_path: &Path) -> PathBuf {
    let mut name = checkpoint_path.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}
