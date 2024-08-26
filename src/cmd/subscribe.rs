use crate::cmd::{Parser, ParserError, Unknown};
use crate::{Command, Connection, Db, Frame, Shutdown};

use bytes::Bytes;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// 订阅客户端到一个或多个频道。
///
/// 一旦客户端进入订阅状态，它不应该发出任何其他命令，除了额外的 SUBSCRIBE、PSUBSCRIBE、UNSUBSCRIBE、PUNSUBSCRIBE、PING 和 QUIT 命令。
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// 取消订阅客户端从一个或多个频道。
///
/// 当没有指定频道时，客户端将从所有先前订阅的频道中取消订阅。
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// 消息流。该流从 `broadcast::Receiver` 接收消息。我们使用 `stream!` 创建一个消费消息的 `Stream`。
/// 因为 `stream!` 值不能被命名，所以我们使用特征对象将流装箱。
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// 创建一个新的 `Subscribe` 命令来监听指定的频道。
    pub(crate) fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    /// 将 `Subscribe` 命令应用于指定的 `Db` 实例。
    ///
    /// 此函数是入口点，包括初始的订阅频道列表。客户端可能会接收到额外的 `subscribe` 和 `unsubscribe` 命令，
    /// 并且订阅列表会相应更新。
    ///
    /// [here]: https://redis.io/topics/pubsub
    pub(crate) async fn apply(mut self, db: &Db, dst: &mut Connection, shutdown: &mut Shutdown) -> crate::Result<()> {
        // 每个单独的频道订阅都使用 `sync::broadcast` 频道处理。消息然后被分发到所有当前订阅频道的客户端。
        //
        // 单个客户端可以订阅多个频道，并且可以动态地添加和删除其订阅集中的频道。为了解决这个问题，
        // 使用 `StreamMap` 来跟踪活动订阅。`StreamMap` 合并来自各个广播频道的消息。
        let mut subscriptions = StreamMap::new();

        loop {
            // `self.channels` 用于跟踪要订阅的额外频道。当在 `apply` 执行期间接收到新的 `SUBSCRIBE` 命令时，
            // 新的频道会被推入这个 vec。
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // 等待以下情况之一发生：
            //
            // - 从订阅的频道接收消息。
            // - 从客户端接收订阅或取消订阅命令。
            // - 服务器关闭信号。
            select! {
                // 从订阅的频道接收消息
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // 这发生在远程客户端断开连接时。
                        None => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };
        }
    }
}

/// 从接收到的帧中解析出一个 `Subscribe` 实例。
///
/// `Parse` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。此时，整个帧已经从套接字接收到。
///
/// `SUBSCRIBE` 字符串已经被消费。
///
/// # 返回值
///
/// 成功时返回 `Subscribe` 值。如果帧格式错误，则返回 `Err`。
///
/// # 格式
///
/// 期望一个包含两个或更多条目的数组帧。
///
/// ```text
/// SUBSCRIBE channel [channel ...]
/// ```
impl TryFrom<&mut Parser> for Subscribe {
    type Error = crate::Error;

    fn try_from(parse: &mut Parser) -> crate::Result<Self> {
        use ParserError::EndOfStream;

        // `SUBSCRIBE` 字符串已经被消费。此时，`parse` 中剩下一个或多个字符串。
        // 这些代表要订阅的频道。
        //
        // 提取第一个字符串。如果没有，则帧格式错误，错误会冒泡。
        let mut channels = vec![parse.next_string()?];

        // 现在，帧的其余部分被消费。每个值必须是字符串，否则帧格式错误。
        // 一旦帧中的所有值都被消费，命令就完全解析了。
        loop {
            match parse.next_string() {
                // 从 `parse` 中消费了一个字符串，将其推入要订阅的频道列表中。
                Ok(s) => channels.push(s),
                // `EndOfStream` 错误表示没有更多数据可解析。
                Err(EndOfStream) => break,
                // 所有其他错误都会冒泡，导致连接被终止。
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Self { channels })
    }
}

/// 将命令转换为等效的 `Frame`。
///
/// 这是由客户端在编码 `Subscribe` 命令以发送到服务器时调用的。
impl From<Subscribe> for Frame {
    fn from(subscribe: Subscribe) -> Self {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in subscribe.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // 订阅频道。
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // 如果我们在消费消息时滞后了，只需恢复。
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // 在此客户端的订阅集中跟踪订阅。
    subscriptions.insert(channel_name.clone(), rx);

    // 响应成功订阅
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// 处理在 `Subscribe::apply` 内接收到的命令。在此上下文中仅允许订阅和取消订阅命令。
///
/// 任何新的订阅都被附加到 `subscribe_to` 而不是修改 `subscriptions`。
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {
    // 从客户端接收到一个命令。
    //
    // 在此上下文中仅允许 `SUBSCRIBE` 和 `UNSUBSCRIBE` 命令。
    match Command::try_from(frame)? {
        Command::Subscribe(subscribe) => {
            // `apply` 方法将订阅我们添加到此向量中的频道。
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // 如果没有指定频道，这请求从 **所有** 频道取消订阅。为了实现这一点，
            // `unsubscribe.channels` vec 被填充为当前订阅的频道列表。
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions.keys().map(|channel_name| channel_name.to_string()).collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

/// 创建订阅请求的响应。
///
/// 所有这些函数都将 `channel_name` 作为 `String` 而不是 `&str`，因为 `Bytes::from` 可以重用 `String` 中的分配，
/// 并且使用 `&str` 会要求复制数据。这允许调用者决定是否克隆频道名称。
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// 创建取消订阅请求的响应。
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// 创建一个消息，通知客户端关于其订阅的频道上的新消息。
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    /// 创建一个带有给定 `channels` 的新 `Unsubscribe` 命令。
    pub(crate) fn new(channels: &[String]) -> Self {
        Self {
            channels: channels.to_vec(),
        }
    }
}

/// 从接收到的帧中解析出一个 `Unsubscribe` 实例。
///
/// `Parse` 参数提供了一个类似游标的 API 来从 `Frame` 中读取字段。此时，整个帧已经从套接字接收到。
///
/// `UNSUBSCRIBE` 字符串已经被消费。
///
/// # 返回值
///
/// 成功时返回 `Unsubscribe` 值。如果帧格式错误，则返回 `Err`。
///
/// # 格式
///
/// 期望一个包含至少一个条目的数组帧。
///
/// ```text
/// UNSUBSCRIBE [channel [channel ...]]
/// ```
impl TryFrom<&mut Parser> for Unsubscribe {
    type Error = crate::Error;

    fn try_from(parser: &mut Parser) -> crate::Result<Self> {
        use ParserError::EndOfStream;

        // 可能没有列出任何频道，因此从一个空的 vec 开始。
        let mut channels = vec![];

        // 帧中的每个条目必须是字符串，否则帧格式错误。
        // 一旦帧中的所有值都被消费，命令就完全解析了。
        loop {
            match parser.next_string() {
                // 从 `parse` 中消费了一个字符串，将其推入要取消订阅的频道列表中。
                Ok(s) => channels.push(s),
                // `EndOfStream` 错误表示没有更多数据可解析。
                Err(EndOfStream) => break,
                // 所有其他错误都会冒泡，导致连接被终止。
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Self { channels })
    }
}

/// 将命令转换为等效的 `Frame`。
///
/// 这是由客户端在编码 `Unsubscribe` 命令以发送到服务器时调用的。
impl From<Unsubscribe> for Frame {
    fn from(unsubscribe: Unsubscribe) -> Self {
        let mut frame = Self::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in unsubscribe.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
