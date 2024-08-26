use crate::{Connection, Db, Frame, Parser};

use bytes::Bytes;
use tracing::{debug, instrument};

/// 获取键的值。
///
/// 如果键不存在，则返回特殊值 nil。如果存储在键中的值不是字符串，则返回错误，因为 GET 只处理字符串值。
#[derive(Debug)]
pub struct Get {
    /// 要获取的键的名称
    key: String,
}

impl Get {
    /// 创建一个新的 `Get` 命令以获取 `key`。
    pub fn new(key: impl ToString) -> Self {
        Self { key: key.to_string() }
    }

    /// 将 `Get` 命令应用于指定的 `Db` 实例。
    ///
    /// 响应写入 `dst`。这是由服务器调用以执行接收到的命令。
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 从共享数据库状态中获取值
        let response = if let Some(value) = db.get(&self.key) {
            // 如果存在值，则以“bulk”格式写入客户端。
            Frame::Bulk(value)
        } else {
            // 如果没有值，则写入 `Null`。
            Frame::Null
        };

        debug!(?response);

        // 将响应写回客户端
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// 从接收到的帧中解析出一个 `Get` 实例。
///
/// `Parse` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。此时，整个帧已经从套接字接收到。
///
/// `GET` 字符串已经被消费。
///
/// # 返回值
///
/// 成功时返回 `Get` 值。如果帧格式错误，则返回 `Err`。
///
/// # 格式
///
/// 期望一个包含两个条目的数组帧。
///
/// ```text
/// GET key
/// ```
impl TryFrom<&mut Parser> for Get {
    type Error = crate::Error;

    fn try_from(parser: &mut Parser) -> crate::Result<Self> {
        // `GET` 字符串已经被消费。下一个值是要获取的键的名称。如果下一个值不是字符串或输入已完全消费，则返回错误。
        let key = parser.next_string()?;

        Ok(Self { key })
    }
}

/// 将命令转换为等效的 `Frame`。
///
/// 这是由客户端在编码 `Get` 命令以发送到服务器时调用的。
impl From<Get> for Frame {
    fn from(get: Get) -> Self {
        let mut frame = Self::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(get.key.into_bytes()));

        frame
    }
}
