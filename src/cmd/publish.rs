use crate::{Connection, Db, Frame, Parser};

use bytes::Bytes;

/// 向指定频道发布消息。
///
/// 将消息发送到频道，而无需了解各个消费者。
/// 消费者可以订阅频道以接收消息。
///
/// 频道名称与键值命名空间无关。在名为 "foo" 的频道上发布与设置 "foo" 键无关。
#[derive(Debug)]
pub struct Publish {
    /// 要发布消息的频道名称。
    channel: String,

    /// 要发布的消息。
    message: Bytes,
}

impl Publish {
    /// 创建一个新的 `Publish` 命令，该命令在 `channel` 上发送 `message`。
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Self {
        Self {
            channel: channel.to_string(),
            message,
        }
    }

    /// 将 `Publish` 命令应用到指定的 `Db` 实例。
    ///
    /// 响应写入 `dst`。这是由服务器调用以执行接收到的命令。
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 共享状态包含所有活动频道的 `tokio::sync::broadcast::Sender`。
        // 调用 `db.publish` 将消息分发到相应的频道。
        //
        // 返回当前在频道上监听的订阅者数量。这并不意味着 `num_subscriber` 频道将接收消息。
        // 订阅者可能在接收消息之前掉线。因此，`num_subscribers` 只能用作“提示”。
        let num_subscribers = db.publish(&self.channel, self.message);

        // 订阅者数量作为发布请求的响应返回。
        let response = Frame::Integer(num_subscribers as u64);

        // 将帧写入客户端。
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// 从接收到的帧解析出一个 `Publish` 实例。
///
/// `Parse` 参数提供了一个类似游标的 API，用于从 `Frame` 中读取字段。
/// 此时，整个帧已经从套接字接收。
///
/// `PUBLISH` 字符串已经被消费。
///
/// # 返回值
///
/// 成功时返回 `Publish` 值。如果帧格式错误，则返回 `Err`。
///
/// # 格式
///
/// 期望一个包含三个条目的数组帧。
///
/// ```text
/// PUBLISH channel message
/// ```
impl TryFrom<&mut Parser> for Publish {
    type Error = crate::Error;

    fn try_from(parser: &mut Parser) -> crate::Result<Self> {
        // `PUBLISH` 字符串已经被消费。提取 `channel` 和 `message` 值。
        //
        // `channel` 必须是一个有效的字符串。
        let channel = parser.next_string()?;

        // `message` 是任意字节。
        let message = parser.next_bytes()?;

        Ok(Self { channel, message })
    }
}

/// 将命令转换为等效的 `Frame`。
///
/// 这是客户端在编码 `Publish` 命令以发送到服务器时调用的。
impl From<Publish> for Frame {
    fn from(publish: Publish) -> Self {
        let mut frame = Self::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(publish.channel.into_bytes()));
        frame.push_bulk(publish.message);

        frame
    }
}
