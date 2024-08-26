use crate::cmd::{Parser, ParserError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
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
pub struct Set {
    /// 查找键
    key: String,
    /// 要存储的值
    value: Bytes,
    /// 键的过期时间
    expire: Option<Duration>,
}

impl Set {
    /// 创建一个新的 `Set` 命令，将 `key` 设置为 `value`。
    ///
    /// 如果 `expire` 是 `Some`，则值应在指定的持续时间后过期。
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Self {
        Self {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// 将 `Set` 命令应用于指定的 `Db` 实例。
    ///
    /// 响应写入 `dst`。这是由服务器调用以执行接收到的命令。
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 在共享数据库状态中设置值。
        db.set(self.key, self.value, self.expire);

        // 创建一个成功响应并将其写入 `dst`。
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// 从接收到的帧中解析出一个 `Set` 实例。
///
/// `Parse` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。此时，整个帧已经从套接字接收到。
///
/// `SET` 字符串已经被消费。
///
/// # 返回值
///
/// 成功时返回 `Set` 值。如果帧格式错误，则返回 `Err`。
///
/// # 格式
///
/// 期望一个包含至少 3 个条目的数组帧。
///
/// ```text
/// SET key value [EX seconds|PX milliseconds]
/// ```
impl TryFrom<&mut Parser> for Set {
    type Error = crate::Error;

    fn try_from(parser: &mut Parser) -> crate::Result<Self> {
        use ParserError::EndOfStream;

        // 读取要设置的键。这是一个必填字段
        let key = parser.next_string()?;
        // 读取要设置的值。这是一个必填字段。
        let value = parser.next_bytes()?;
        // 过期时间是可选的。如果没有其他内容，则为 `None`。
        let mut expire = None;
        // 尝试解析另一个字符串。
        match parser.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // 过期时间以秒为单位指定。下一个值是一个整数。
                let secs = parser.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // 过期时间以毫秒为单位指定。下一个值是一个整数。
                let ms = parser.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            // 目前，mini-redis 不支持任何其他 SET 选项。此处的错误会导致连接被终止。
            // 其他连接将继续正常运行。
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            // `EndOfStream` 错误表示没有更多数据可解析。在这种情况下，这是正常的运行时情况，
            // 表示没有指定的 `SET` 选项。
            Err(EndOfStream) => {}
            // 所有其他错误都会冒泡，导致连接被终止。
            Err(err) => return Err(err.into()),
        }

        Ok(Self { key, value, expire })
    }
}

/// 将命令转换为等效的 `Frame`。
///
/// 这是由客户端在编码 `Set` 命令以发送到服务器时调用的。
impl From<Set> for Frame {
    fn from(set: Set) -> Self {
        let mut frame = Self::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(set.key.into_bytes()));
        frame.push_bulk(set.value);
        if let Some(ms) = set.expire {
            // Redis 协议中的过期时间可以通过两种方式指定
            // 1. SET key value EX seconds
            // 2. SET key value PX milliseconds
            // 我们选择第二种方式，因为它允许更高的精度，并且
            // src/bin/cli.rs 将过期参数解析为毫秒
            // 在 duration_from_ms_str() 中
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(ms.as_millis() as u64);
        }

        frame
    }
}
