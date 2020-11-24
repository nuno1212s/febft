use futures_codec::CborCodec;
use futures::io::{AsyncRead, AsyncWrite};

pub type Serializer<W> = super::Serializer<W, CborCodec<super::Message, super::Message>>;

pub type Deserializer<R> = super::Deserializer<R, CborCodec<super::Message, super::Message>>;

pub fn new_serializer<W: Unpin + AsyncWrite>(writer: W) -> Serializer<W> {
    Serializer::new(writer, CborCodec::new())
}

pub fn new_deserializer<R: Unpin + AsyncRead>(reader: R) -> Deserializer<R> {
    Deserializer::new(reader, CborCodec::new())
}
