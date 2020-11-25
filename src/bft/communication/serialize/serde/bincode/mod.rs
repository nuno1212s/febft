use std::marker::PhantomData;

use serde::{Serialize, Deserialize};
use futures::io::{AsyncRead, AsyncWrite};
use futures_codec::{Encoder, Decoder, BytesMut};
use bytes::{Buf, BufMut};

use crate::bft::error::*;

pub type Serializer<W> = super::Serializer<W, BincodeCodec<super::Message, super::Message>>;

pub type Deserializer<R> = super::Deserializer<R, BincodeCodec<super::Message, super::Message>>;

pub struct BincodeCodec<Enc, Dec> {
    _phantom: PhantomData<(Enc, Dec)>,
}

pub fn new_serializer<W: Unpin + AsyncWrite>(writer: W) -> Serializer<W> {
    Serializer::new(writer, BincodeCodec::new())
}

pub fn new_deserializer<R: Unpin + AsyncRead>(reader: R) -> Deserializer<R> {
    Deserializer::new(reader, BincodeCodec::new())
}

impl<Enc, Dec> Decoder for BincodeCodec<Enc, Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
    for<'de> Enc: Serialize + 'static,
{
    type Item = Dec;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        // Build cursor
        let mut c = std::io::Cursor::new(&buf);

        // Attempt deserialization
        let res: std::result::Result<Dec, _> = bincode::deserialize_from(&mut c);

        // If we ran out before parsing, return none and try again later
        let res = match res {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                match *e {
                    bincode::ErrorKind::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        Ok(None)
                    },
                    _ => {
                        Err(Error::wrapped(ErrorKind::CommunicationSerializeSerdeBincode, e))
                    },
                }
            },
        };

        // Update offset from iterator
        let offset = c.position() as usize;

        // Advance buffer
        buf.advance(offset);

        res
    }
}

impl<Enc, Dec> Encoder for BincodeCodec<Enc, Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
    for<'de> Enc: Serialize + 'static,
{
    type Item = Enc;
    type Error = Error;

    fn encode(&mut self, data: Self::Item, buf: &mut BytesMut) -> Result<()> {
        // Encode bincode
        let j = bincode::serialize(&data)
            .wrapped(ErrorKind::CommunicationSerializeSerdeBincode)?;

        // Write to buffer
        buf.reserve(j.len());
        buf.put_slice(&j);

        Ok(())
    }
}

impl<Enc, Dec> Clone for BincodeCodec<Enc, Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
    for<'de> Enc: Serialize + 'static,
{
    fn clone(&self) -> Self {
        BincodeCodec::new()
    }
}

impl<Enc, Dec> BincodeCodec<Enc, Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
    for<'de> Enc: Serialize + 'static,
{
    pub fn new() -> Self {
        Self { _phantom: PhantomData }
    }
}
