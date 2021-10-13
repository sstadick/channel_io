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
use glommio::LocalExecutorBuilder;

const GLOMMIO_BUF_SIZE: usize = 512 << 10;
pub struct GlommioReader {
    handle: JoinHandle<()>,
    reader: ChannelReader,
}

impl GlommioReader {
    fn new<P: AsRef<Path>>(file: P) -> Self {
        Self::with_capacity(file, GLOMMIO_BUF_SIZE)
    }

    // TODO: add pin ability
    // TODO: allow config channel size
    // TODO: allow config wait time
    fn with_capacity<P: AsRef<Path>>(file: P, capacity: usize) -> Self {
        let (tx, rx) = bounded(10);
        let handle = LocalExecutorBuilder::new()
            .spin_before_park(Duration::from_millis(10))
            .spawn(move || async move {
                let file = DmaFile::open(&dio_filename).await.unwrap();
                let stream = DmaStreamReaderBuilder::new(file)
                    .with_read_ahead(10)
                    .with_buffer_size(512 << 10)
                    .build();

                let mut bytes = BytesMut::with_capacity(capacity);

                loop {
                    let buffer = stream.get_buffer_aligned(capacity as _).await.unwrap();
                    bytes.extend_from_slice(&buffer[..]);
                    let to_send = bytes.split();
                    tx.send(to_send.freeze()).unwrap();

                    if buffer.len() < capacity {
                        break;
                    }
                }
            })
            .unwrap();

        Self {
            handle,
            reader: ChannelReader::new(rx),
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
        self.handle.join().unwrap()
    }
}

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
