use crate::{Connection, Frame, Parser, ParserError};
use bytes::Bytes;
use tracing::{debug, instrument};

/// 如果没有提供参数，则返回 PONG，否则返回参数的副本作为 bulk。
///
/// 此命令通常用于测试连接是否仍然存在，或测量延迟。
#[derive(Debug, Default)]
pub struct Ping {
    /// 要返回的可选消息
    msg: Option<Bytes>,
}

impl Ping {
    /// 创建一个带有可选 `msg` 的新 `Ping` 命令。
    pub fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }

    /// 应用 `Ping` 命令并返回消息。
    ///
    /// 响应写入 `dst`。这是由服务器调用以执行接收到的命令。
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            Some(msg) => Frame::Bulk(msg),
            None => Frame::Simple("PONG".to_string()),
        };

        debug!(?response);
        // 将响应写回客户端
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// 从接收到的帧中解析出一个 `Ping` 实例。
///
/// `Parse` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。此时，整个帧已经从套接字接收到。
///
/// `PING` 字符串已经被消费。
///
/// # 返回值
///
/// 成功时返回 `Ping` 值。如果帧格式错误，则返回 `Err`。
///
/// # 格式
///
/// 期望一个包含 `PING` 和可选消息的数组帧。
///
/// ```text
/// PING [message]
/// ```
impl TryFrom<&mut Parser> for Ping {
    type Error = crate::Error;

    fn try_from(parse: &mut Parser) -> crate::Result<Self> {
        use ParserError::EndOfStream;

        match parse.next_bytes() {
            Ok(msg) => Ok(Self::new(Some(msg))),
            Err(EndOfStream) => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }
}

/// 将命令转换为等效的 `Frame`。
///
/// 这是由客户端在编码 `Ping` 命令以发送到服务器时调用的。
impl From<Ping> for Frame {
    fn from(ping: Ping) -> Self {
        let mut frame = Self::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = ping.msg {
            frame.push_bulk(msg);
        }

        frame
    }
}
