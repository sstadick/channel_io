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
use std::io::Read;

use bytes::{Buf, Bytes};
use flume::Receiver;

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
                eprintln!(
                    "Status of rx: disconnected - {}, empty - {}",
                    self.rx.is_disconnected(),
                    self.rx.is_empty()
                );
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

    use crate::ChannelReader;

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
}
