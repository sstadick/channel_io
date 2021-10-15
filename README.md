# 📖 channel_io

<p align="center">
  <a href="https://github.com/sstadick/channel_io/actions?query=workflow%3ACheck"><img src="https://github.com/sstadick/channel_io/workflows/Check/badge.svg" alt="Build Status"></a>
  <img src="https://img.shields.io/crates/l/channel_io.svg" alt="license">
  <a href="https://crates.io/crates/channel_io"><img src="https://img.shields.io/crates/v/channel_io.svg?colorB=319e8c" alt="Version info"></a><br>
</p>

A small helper library to convert a `flume` channel of `bytes` into a `ChannelReader` that implements `Read`.

## Example

```rust
use std::io::Read;

use bytes::Bytes;
use channel_reader::ChannelReader;
use flume::bounded;

fn main() {
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
```

## Why flume?

The goal is to bridge an async reader-like-thing (in this case a DmaStreamReader in Glommio) into a synchronous reader.

## Why bytes?

I like the API, and in theory as chunks are dropped on the sync side of the sender depending on how the caller is doing things the
memory can be reused.
