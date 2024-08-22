use crate::frame::Frame;

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// 从远程对等方发送和接收 `Frame` 值。
///
/// 在实现网络协议时，协议上的消息通常由几个较小的消息组成，称为帧。
/// `Connection` 的目的是在底层的 `TcpStream` 上读取和写入帧。
///
/// 为了读取帧，`Connection` 使用内部缓冲区，直到有足够的字节来创建完整的帧。
/// 一旦发生这种情况，`Connection` 创建帧并将其返回给调用者。
///
/// 在发送帧时，帧首先被编码到写缓冲区中。然后将写缓冲区的内容写入套接字。
#[derive(Debug)]
pub struct Connection {
    // `TcpStream`。它被 `BufWriter` 装饰，提供写级别的缓冲。
    // Tokio 提供的 `BufWriter` 实现足以满足我们的需求。
    stream: BufWriter<TcpStream>,
    // 用于读取帧的缓冲区。
    buffer: BytesMut,
}

impl Connection {
    /// 创建一个新的 `Connection`，由 `socket` 支持。读写缓冲区被初始化。
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            // 默认使用 4KB 的读取缓冲区。对于 mini redis 的用例，这是可以的。
            // 然而，实际应用程序将希望根据其特定用例调整此值。很有可能较大的读取缓冲区会更好。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 从底层流中读取单个 `Frame` 值。
    ///
    /// 该函数等待，直到它检索到足够的数据来解析帧。
    /// 在帧解析后，读取缓冲区中剩余的任何数据将保留在下一次调用 `read_frame` 时使用。
    ///
    /// # 返回值
    ///
    /// 成功时，返回接收到的帧。如果 `TcpStream` 以不破坏帧的方式关闭，则返回 `None`。
    /// 否则，返回错误。
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 尝试从缓冲数据中解析帧。如果已缓冲足够的数据，则返回帧。
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            // 缓冲的数据不足以读取帧。尝试从套接字读取更多数据。
            //
            // 成功时，返回字节数。`0` 表示“流结束”。
            if 0 != self.stream.read_buf(&mut self.buffer).await? {
                continue;
            }
            // 远程关闭了连接。为了实现干净的关闭，读取缓冲区中不应有数据。
            // 如果有，这意味着对等方在发送帧时关闭了套接字。
            if self.buffer.is_empty() {
                return Ok(None);
            } else {
                return Err("connection reset by peer".into());
            }
        }
    }

    /// 尝试从缓冲区解析帧。如果缓冲区包含足够的数据，则返回帧并从缓冲区中移除数据。
    /// 如果缓冲的数据不足，则返回 `Ok(None)`。如果缓冲的数据不是有效的帧，则返回 `Err`。
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use crate::frame::FrameError::Incomplete;

        // Cursor 用于跟踪缓冲区中的“当前位置”。Cursor 还实现了 `bytes` crate 中的 `Buf`，
        // 提供了许多处理字节的有用工具。
        let mut buf = Cursor::new(&self.buffer[..]);
        // 第一步是检查是否已缓冲足够的数据来解析单个帧。
        // 这一步通常比进行完整的帧解析要快得多，并且允许我们跳过分配数据结构来保存帧数据，
        // 除非我们知道已接收到完整的帧。
        match Frame::check(&mut buf) {
            Ok(_) => {
                // `check` 函数将把光标推进到帧的末尾。
                // 由于在调用 `Frame::check` 之前光标的位置设置为零，
                // 我们通过检查光标位置来获取帧的长度。
                let len = buf.position() as usize;
                // 在将光标传递给 `Frame::parse` 之前，将位置重置为零。
                buf.set_position(0);
                // 从缓冲区解析帧。这会分配必要的结构来表示帧并返回帧值。
                //
                // 如果编码的帧表示无效，则返回错误。
                // 这应该终止**当前**连接，但不应影响任何其他连接的客户端。
                let frame = Frame::from(&mut buf);
                // 丢弃读取缓冲区中已解析的数据。
                //
                // 当调用读取缓冲区上的 `advance` 时，所有数据都会被丢弃，直到 `len`。
                // 具体细节由 `BytesMut` 处理。这通常通过移动内部光标来完成，但也可能通过重新分配和复制数据来完成。
                self.buffer.advance(len);

                // 将解析的帧返回给调用者。
                Ok(Some(frame))
            }
            // 读取缓冲区中没有足够的数据来解析单个帧。
            // 我们必须等待从套接字接收更多数据。
            // 读取套接字将在此 `match` 语句之后进行。
            //
            // 我们不希望从这里返回 `Err`，因为这种“错误”是预期的运行时条件。
            Err(Incomplete) => Ok(None),
            // 解析帧时遇到错误。连接现在处于无效状态。
            // 从这里返回 `Err` 将导致连接关闭。
            Err(e) => Err(e.into()),
        }
    }

    /// 将单个 `Frame` 值写入底层流。
    ///
    /// 使用 `AsyncWrite` 提供的各种 `write_*` 函数将 `Frame` 值写入套接字。
    /// 直接在 `TcpStream` 上调用这些函数**不**建议，因为这会导致大量的系统调用。
    /// 但是，在*缓冲*写流上调用这些函数是可以的。数据将被写入缓冲区。
    /// 一旦缓冲区满了，它将被刷新到底层套接字。
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // 数组通过编码每个条目来编码。所有其他帧类型都被视为文字。
        // 目前，mini-redis 无法编码递归帧结构。有关更多详细信息，请参见下文。
        match frame {
            Frame::Array(value) => {
                // 编码帧类型前缀。对于数组，它是 `*`。
                self.stream.write_u8(b'*').await?;
                // 编码数组的长度。
                self.write_decimal(value.len() as u64).await?;
                // 迭代并编码数组中的每个条目。
                for frame in value.iter() {
                    self.write_value(frame).await?;
                }
            }
            // 帧类型是文字。直接编码值。
            _ => self.write_value(frame).await?,
        }

        // 确保编码的帧被写入套接字。上面的调用是对缓冲流和写入的调用。
        // 调用 `flush` 将缓冲区的剩余内容写入套接字。
        self.stream.flush().await
    }

    /// 将帧文字写入流
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(value) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(value.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(value) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(value.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(value) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*value).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(value) => {
                let len = value.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(value).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 在值中编码 `Array` 不能使用递归策略。
            // 一般来说，异步函数不支持递归。
            // Mini-redis 还不需要编码嵌套数组，所以目前跳过它。
            Frame::Array(_value) => unreachable!(),
        }

        Ok(())
    }

    /// 将十进制帧写入流
    async fn write_decimal(&mut self, value: u64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", value)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
