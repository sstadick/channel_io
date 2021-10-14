#![allow(clippy::missing_panics_doc)]
//! A simple helper library to turn a channel of bytes into a reader.
//!
//! # Example
//! ```rust
//! use std::io::Read;
//!
//! use bytes::Bytes;
//! use channel_reader::ChannelReader;
//! use flume::bounded;
//!
//!
//! let (tx, rx) = bounded(10);
//! let sender_thread = std::thread::spawn(move || {
//!     for i in 0..10 {
//!         let buffer = Bytes::from(vec![i; 10]);
//!         tx.send(buffer).unwrap();
//!     }
//! });
//!
//! sender_thread.join().unwrap();
//!
//! let mut reader = ChannelReader::new(rx);
//! let mut buffer = vec![];
//! reader.read_to_end(&mut buffer).unwrap();
//! assert_eq!(buffer.len(), 100);
//! ```
use std::{
    io::{Read, Write},
    path::PathBuf,
    thread::JoinHandle,
    time::Duration,
};

use bytes::{Buf, Bytes, BytesMut};
use flume::{bounded, Receiver, Sender};
use futures_lite::AsyncReadExt;
use glommio::{
    io::{DmaFile, DmaStreamReaderBuilder},
    LocalExecutorBuilder,
};

const GLOMMIO_BUF_SIZE: usize = 512 << 10;
pub struct GlommioReader {
    handle: Option<JoinHandle<()>>,
    reader: ChannelReader,
    tx_kill: Sender<()>,
}

impl GlommioReader {
    pub fn new<P: Into<PathBuf> + Send>(file: P) -> Self {
        Self::with_capacity(file, GLOMMIO_BUF_SIZE)
    }

    // TODO: add pin ability
    // TODO: allow config channel size
    // TODO: allow config wait time
    // TODO: add a kill signal
    pub fn with_capacity<P: Into<PathBuf> + Send>(file: P, capacity: usize) -> Self {
        let file = file.into();
        let (tx, rx) = bounded(10);
        let (tx_kill, rx_kill) = bounded(1);
        let handle = LocalExecutorBuilder::new()
            // .pin_to_cpu(32)
            .spin_before_park(Duration::from_millis(10))
            .spawn(move || async move {
                let file = DmaFile::open(&file).await.unwrap();
                let mut stream = DmaStreamReaderBuilder::new(file)
                    .with_read_ahead(10)
                    .with_buffer_size(capacity)
                    .build();

                let mut bytes = BytesMut::with_capacity(capacity);

                loop {
                    if rx_kill.try_recv().is_ok() {
                        break;
                    }

                    let buffer = stream.get_buffer_aligned(capacity as _).await.unwrap();
                    bytes.resize(buffer.len(), 0);
                    bytes.copy_from_slice(&buffer[..]);
                    let to_send = bytes.split();
                    tx.send_async(to_send.freeze()).await.unwrap();

                    if buffer.len() < capacity {
                        break;
                    }

                    // let to_send = bytes.split_to(bytes_read);
                    // tx.send_async(to_send.freeze()).await.unwrap();
                }
            })
            .unwrap();

        Self {
            handle: Some(handle),
            reader: ChannelReader::new(rx),
            tx_kill,
        }
    }
}

impl Read for GlommioReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Drop for GlommioReader {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ret = self.tx_kill.try_send(());
            handle.join().expect("Failed to join on glommio thread.");
        }
    }
}

pub const BUFSIZE: usize = 64 * (1 << 10) * 2;

#[derive(Debug)]
pub struct ChannelWriter {
    tx: Sender<Bytes>,
    buffer: BytesMut,
    send_size: usize,
}

impl ChannelWriter {
    pub fn new(tx: Sender<Bytes>) -> Self {
        Self::with_capacity(tx, BUFSIZE)
    }

    pub fn with_capacity(tx: Sender<Bytes>, capacity: usize) -> Self {
        Self {
            tx,
            buffer: BytesMut::with_capacity(capacity),
            send_size: capacity,
        }
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= self.send_size {
            let b = self.buffer.split_to(self.send_size).freeze();
            self.tx.send(b).unwrap();
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let b = self.buffer.split().freeze();
        if !b.is_empty() {
            self.tx.send(b).unwrap();
        }
        Ok(())
    }
}

impl Drop for ChannelWriter {
    fn drop(&mut self) {
        self.flush().unwrap()
    }
}

#[derive(Debug)]
pub struct ChannelReader {
    rx: Receiver<Bytes>,
    buffer: Bytes,
}

impl ChannelReader {
    pub fn new(rx: Receiver<Bytes>) -> Self {
        Self {
            rx,
            buffer: Bytes::new(),
        }
    }
}

impl Read for ChannelReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_read = 0;
        loop {
            let before = self.buffer.remaining();
            if before > buf.len() - total_read {
                self.buffer.copy_to_slice(&mut buf[total_read..]);
            } else if !self.buffer.is_empty() {
                self.buffer
                    .copy_to_slice(&mut buf[total_read..total_read + before]);
            }
            let after = self.buffer.remaining();
            total_read += before - after;

            if total_read == buf.len() {
                break;
            } else if total_read <= buf.len() {
                let mut new_bytes = match self.rx.recv() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        if self.rx.is_disconnected() && self.rx.is_empty() {
                            break;
                        } else {
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                        }
                    }
                };

                std::mem::swap(&mut self.buffer, &mut new_bytes);
            }
        }
        Ok(total_read)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use bytes::Bytes;
    use flume::bounded;

    use crate::{ChannelReader, GlommioReader};

    #[test]
    fn simple_test() {
        let (tx, rx) = bounded(10);
        let sender_thread = std::thread::spawn(move || {
            for i in 0..10 {
                let buffer = Bytes::from(vec![i; 10]);
                tx.send(buffer).unwrap();
            }
        });
        sender_thread.join().unwrap();

        let mut reader = ChannelReader::new(rx);
        let mut buffer = vec![];
        reader.read_to_end(&mut buffer).unwrap();
        assert_eq!(buffer.len(), 100);
    }

    #[test]
    fn close_early_test() {
        let (tx, rx) = bounded(10);
        let sender_thread = std::thread::spawn(move || {
            for i in 0..10 {
                let buffer = Bytes::from(vec![i; 10]);
                tx.send(buffer).unwrap();
            }
        });
        sender_thread.join().unwrap();

        let mut reader = ChannelReader::new(rx);
        let mut buffer = vec![0; 120];
        let bytes_read = reader.read(&mut buffer).unwrap();
        assert_eq!(bytes_read, 100);
    }

    #[test]
    fn simple_test_glommio() {
        let mut reader = GlommioReader::new("/dev/urandom");
        let mut buffer = vec![0; 100];
        reader.read_exact(&mut buffer).unwrap();
        assert_eq!(buffer.len(), 100);
    }
}
