use std::io::Cursor;

use crate::{decode::DecodeResult, response_message::ResponseMessage};
use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    decode::{Decode, DecodeError},
    request_message::RequestMessage,
};

pub struct Connection {
    socket: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Connection {
            socket: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_request(&mut self) -> crate::Result<Option<RequestMessage>> {
        loop {
            if let Some(request) = self.parse_request()? {
                return Ok(Some(request));
            } else if 0 == self.socket.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_request(&mut self) -> DecodeResult<Option<RequestMessage>> {
        let mut buffer = Cursor::new(self.buffer.as_ref());
        match RequestMessage::decode(&mut buffer) {
            Ok(request) => {
                let pos = buffer.position() as usize;
                self.buffer.advance(pos);
                Ok(Some(request))
            }
            Err(DecodeError::Incomplete(_err)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn write_response(&mut self, response: &mut ResponseMessage) -> crate::Result<()> {
        let encode_response = response.as_bytes();
        self.socket.write_all(&encode_response).await?;
        self.socket.flush().await?;
        Ok(())
    }
}
