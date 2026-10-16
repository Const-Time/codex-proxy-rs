//! 有界文件日志队列：普通日志拥堵时丢弃整条事件，关键诊断背压；退出排空并报告健康状态。

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use futures::future::BoxFuture;
use gateway_core::health::{HealthProbe, HealthState};
use tracing_subscriber::fmt::MakeWriter;

use super::writer::RotatingLogWriter;

const QUEUE_CAPACITY: usize = 64;

enum Command {
    Write(Vec<u8>),
    Shutdown,
}

#[derive(Default)]
pub(super) struct LogHealth {
    dropped_records: AtomicU64,
    write_failures: AtomicU64,
    maintenance_failures: AtomicU64,
}

impl LogHealth {
    pub(super) fn write_failed(&self, kind: io::ErrorKind) {
        if self.write_failures.fetch_add(1, Ordering::Relaxed) == 0 {
            eprintln!("file_logging: log completeness compromised; write failure ({kind:?})");
        }
    }

    pub(super) fn maintenance_failed(&self, kind: io::ErrorKind) {
        if self.maintenance_failures.fetch_add(1, Ordering::Relaxed) == 0 {
            eprintln!("file_logging: archive/retention maintenance failed ({kind:?})");
        }
    }
}

impl HealthProbe for LogHealth {
    fn name(&self) -> &'static str {
        "file_logging"
    }

    fn check(&self) -> BoxFuture<'_, HealthState> {
        Box::pin(async move {
            let failures = self.write_failures.load(Ordering::Relaxed);
            if failures > 0 {
                return HealthState::Unhealthy(format!(
                    "log completeness compromised: {failures} write/flush failures since startup"
                ));
            }
            let failures = self.maintenance_failures.load(Ordering::Relaxed);
            if failures > 0 {
                return HealthState::Degraded(format!(
                    "log archive/retention maintenance failed {failures} times since startup"
                ));
            }
            let dropped = self.dropped_records.load(Ordering::Relaxed);
            if dropped > 0 {
                return HealthState::Degraded(format!(
                    "application log overload: {dropped} records dropped since startup"
                ));
            }
            HealthState::Healthy
        })
    }
}

#[derive(Clone)]
pub(super) struct FileLogSink {
    sender: mpsc::SyncSender<Command>,
    health: Arc<LogHealth>,
    lossless: bool,
}

pub(super) struct FileLogGuard {
    sender: mpsc::SyncSender<Command>,
    worker: Option<JoinHandle<()>>,
    health: Arc<LogHealth>,
}

impl FileLogSink {
    pub(super) fn spawn(
        mut writer: RotatingLogWriter,
        name: &'static str,
        health: Arc<LogHealth>,
        lossless: bool,
    ) -> io::Result<(Self, FileLogGuard)> {
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let worker_health = Arc::clone(&health);
        let worker = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    match command {
                        Command::Write(bytes) => {
                            if let Err(error) = writer.write_all(&bytes) {
                                worker_health.write_failed(error.kind());
                            }
                        }
                        Command::Shutdown => break,
                    }
                }
                if let Err(error) = writer.sync() {
                    worker_health.write_failed(error.kind());
                }
            })?;
        Ok((
            Self {
                sender: sender.clone(),
                health: Arc::clone(&health),
                lossless,
            },
            FileLogGuard {
                sender,
                worker: Some(worker),
                health,
            },
        ))
    }
}

impl<'a> MakeWriter<'a> for FileLogSink {
    type Writer = FileLogRecord;

    fn make_writer(&'a self) -> Self::Writer {
        FileLogRecord {
            sink: self.clone(),
            bytes: Vec::new(),
            oversized: false,
        }
    }
}

pub(super) struct FileLogRecord {
    sink: FileLogSink,
    bytes: Vec<u8>,
    oversized: bool,
}

impl Write for FileLogRecord {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if !self.sink.lossless
            && (self.oversized || self.bytes.len().saturating_add(buffer.len()) > 64 * 1024)
        {
            self.oversized = true;
            self.bytes.clear();
        } else {
            self.bytes.extend_from_slice(buffer);
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Files are unbuffered. The guard is the lifecycle owner of queue drain and sync.
        Ok(())
    }
}

impl Drop for FileLogRecord {
    fn drop(&mut self) {
        // Enqueue a whole event, never fragments which could corrupt adjacent JSON lines.
        if self.oversized {
            self.sink
                .health
                .dropped_records
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let command = Command::Write(std::mem::take(&mut self.bytes));
        if self.sink.lossless {
            if self.sink.sender.send(command).is_err() {
                self.sink.health.write_failed(io::ErrorKind::BrokenPipe);
            }
        } else {
            match self.sink.sender.try_send(command) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    self.sink
                        .health
                        .dropped_records
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    self.sink.health.write_failed(io::ErrorKind::BrokenPipe)
                }
            }
        }
    }
}

impl Drop for FileLogGuard {
    fn drop(&mut self) {
        // Shutdown follows every accepted record. Unlike a timed guard, joining cannot
        // silently abandon queued log records during a normal process shutdown.
        let _ = self.sender.send(Command::Shutdown);
        if self
            .worker
            .take()
            .is_some_and(|worker| worker.join().is_err())
        {
            self.health.write_failed(io::ErrorKind::BrokenPipe);
        }
    }
}
