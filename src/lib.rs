#![forbid(unsafe_code)]
//! A simple helper library to convert Senders/Receivers into Writers/Readers.
//!
//! # Examples
//!
//! ## Using a receiver as a reader
//!
//! ```rust
//! use std::io::Read;
//!
//! use bytes::Bytes;
//! use channel_io::ChannelReader;
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
//!
//! ## Using a sender as a writer
//!
//! ```rust
//! use std::io::Write;
//!
//! use bytes::Bytes;
//! use channel_io::ChannelWriter;
//! use flume::{bounded, Receiver, Sender};
//!
//!
//! let (tx, rx): (Sender<Bytes>, Receiver<Bytes>) = bounded(10);
//! let sender_thread = std::thread::spawn(move || {
//!     let mut total_read = 0;
//!     while let Ok(bytes) = rx.recv() {
//!         total_read += bytes.len();
//!     }
//!     total_read
//! });
//!
//! let mut writer = ChannelWriter::new(tx);
//! writer.write_all(b"Let's add a happy little tree.").unwrap();
//! writer.write_all(b"And maybe a little snowcap right over here.").unwrap();
//! writer.flush().unwrap();
//! // Note we must drop the writer to dropt he sender to allow the thread to close
//! drop(writer);
//!
//! let bytes_read = sender_thread.join().unwrap();
//! assert_eq!(bytes_read, 73);
//! ```
use std::io::{Read, Write};

use bytes::{Buf, Bytes, BytesMut};
use flume::{Receiver, Sender};

/// The default buffer size to use (128K)
pub const BUFSIZE: usize = 64 * (1 << 10) * 2;

/// A channel writer that writes bytes to a [`Sender<Bytes>`].
///
/// This implements [`Write`] and will send filled [`Bytes`] buffers
/// across the provided [`Sender`].
#[derive(Debug, Clone)]
pub struct ChannelWriter {
    tx: Sender<Bytes>,
    buffer: BytesMut,
    send_size: usize,
}

impl ChannelWriter {
    /// Create a new [`ChannelWriter`]
    #[must_use]
    pub fn new(tx: Sender<Bytes>) -> Self {
        Self::with_capacity(tx, BUFSIZE)
    }

    /// Crate a [`ChannelWriter`] with the given capacity.
    ///
    /// The capacity is used both as the size of the internal buffer, as
    /// well as the cutoff at which to send the bytes across the channel.
    #[must_use]
    pub fn with_capacity(tx: Sender<Bytes>, capacity: usize) -> Self {
        Self {
            tx,
            buffer: BytesMut::with_capacity(capacity),
            send_size: capacity,
        }
    }

    /// Clear the internal buffer
    pub fn reset(&mut self) {
        self.buffer.clear();
    }
}

impl Write for ChannelWriter {
    /// Write the bytes from `buf` to the internal buffer, sending across
    /// the channel when the internal buffer is full.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= self.send_size {
            let b = self.buffer.split_to(self.send_size).freeze();
            self.tx.send(b).unwrap();
        }
        Ok(buf.len())
    }

    /// Flush the buffer, sending any bytes currently in the buffer across
    /// the channel.
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
        self.flush().unwrap();
    }
}

/// A channel reader that reads bytes from a [`Receiver<Bytes>`]
///
/// This implements [`Read`] and will read bytes from the
/// provided [`Receiver`]. This works by pulling a buffer from the
/// channel and holding it internally to satisfy requests of `read`.
/// Therefore the size of the buffer on this reader is dependant on the
/// size of the buffers being sent over the channel.
#[derive(Debug)]
pub struct ChannelReader {
    rx: Receiver<Bytes>,
    buffer: Bytes,
}

impl ChannelReader {
    /// Create a new [`ChannelReader`].
    #[must_use]
    pub fn new(rx: Receiver<Bytes>) -> Self {
        Self {
            rx,
            buffer: Bytes::new(),
        }
    }
}

impl Read for ChannelReader {
    /// Read bytes into the passed in `buf`, returning the number of bytes read in.
    ///
    /// If 0 bytes were read into the passed in file then EOF has been reached.
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
                        }
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
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
    use std::io::{Read, Write};

    use bytes::Bytes;
    use flume::{bounded, Receiver, Sender};

    use crate::{ChannelReader, ChannelWriter};

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
    fn test_simple_writer() {
        let (tx, rx): (Sender<Bytes>, Receiver<Bytes>) = bounded(10);
        let sender_thread = std::thread::spawn(move || {
            let mut total_read = 0;
            while let Ok(bytes) = rx.recv() {
                total_read += bytes.len();
            }
            total_read
        });

        let mut writer = ChannelWriter::new(tx);
        writer.write_all(b"Let's add a happy little tree.").unwrap();
        writer
            .write_all(b"And maybe a little snowcap right over here.")
            .unwrap();
        writer.flush().unwrap();
        // Note we must drop the writer to dropt he sender to allow the thread to close
        drop(writer);
        let bytes_read = sender_thread.join().unwrap();
        assert_eq!(bytes_read, 73);
    }
}
