use crate::cmd::{Parser, ParserError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use tracing::{debug, instrument};

/// 将 `key` 设置为保存字符串 `value`。
///
/// 如果 `key` 已经保存了一个值，则无论其类型如何，都会被覆盖。
/// 任何与键关联的先前生存时间在成功的 SET 操作中都会被丢弃。
///
/// # 选项
///
/// 目前，支持以下选项：
///
/// * EX `seconds` -- 设置指定的过期时间，以秒为单位。
/// * PX `milliseconds` -- 设置指定的过期时间，以毫秒为单位。
#[derive(Debug)]
pub struct Del {
    /// 查找键
    keys: Vec<String>,
}

impl Del {
    /// 创建一个新的 `Set` 命令，将 `key` 设置为 `value`。
    ///
    /// 如果 `expire` 是 `Some`，则值应在指定的持续时间后过期。
    pub fn new(key: Vec<String>) -> Self {
        Self { keys: key }
    }

    /// 将 `Set` 命令应用于指定的 `Db` 实例。
    ///
    /// 响应写入 `dst`。这是由服务器调用以执行接收到的命令。
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 在共享数据库状态中设置值。
        db.del(self.keys);

        // 创建一个成功响应并将其写入 `dst`。
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// 从接收到的帧中解析出一个 `Del` 实例。
///
/// `Parser` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。此时，整个帧已经从套接字接收到。
///
/// `DEL` 字符串已经被消费。
///
/// # 返回值
///
/// 成功时返回 `Del` 值。如果帧格式错误，则返回 `Err`。
///
/// # 格式
///
/// 期望一个包含两个或更多条目的数组帧。
///
/// ```text
/// DEL key1 [key2 ...]
/// ```
impl TryFrom<&mut Parser> for Del {
    type Error = crate::Error;

    fn try_from(parse: &mut Parser) -> crate::Result<Self> {
        use ParserError::EndOfStream;

        // `DEL` 字符串已经被消费。此时，`parse` 中剩下一个或多个字符串。
        // 这些代表要删除的键。
        //
        // 提取第一个字符串。如果没有，则帧格式错误，错误会冒泡。
        let mut keys = vec![parse.next_string()?];

        // 现在，帧的其余部分被消费。每个值必须是字符串，否则帧格式错误。
        // 一旦帧中的所有值都被消费，命令就完全解析了。
        loop {
            match parse.next_string() {
                // 从 `parse` 中消费了一个字符串，将其推入要删除的键列表中。
                Ok(s) => keys.push(s),
                // `EndOfStream` 错误表示没有更多数据可解析。
                Err(EndOfStream) => break,
                // 所有其他错误都会冒泡，导致连接被终止。
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Del { keys })
    }
}

/// 将命令转换为等效的 `Frame`。
///
/// 这是由客户端在编码 `Set` 命令以发送到服务器时调用的。
impl From<Del> for Frame {
    fn from(del: Del) -> Self {
        let mut frame = Self::array();
        frame.push_bulk(Bytes::from("del".as_bytes()));
        for key in del.keys {
            frame.push_bulk(Bytes::from(key.into_bytes()));
        }

        frame
    }
}
