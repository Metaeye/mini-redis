//! 最小阻塞 Redis 客户端实现
//!
//! 提供阻塞连接和发出支持的命令的方法。

use bytes::Bytes;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::runtime::Runtime;

pub use crate::clients::Message;

/// 与 Redis 服务器建立的连接。
///
/// 由单个 `TcpStream` 支持，`BlockingClient` 提供基本的网络客户端功能（无池化、重试等）。
/// 使用 [`connect`](fn@connect) 函数建立连接。
///
/// 请求是通过 `Client` 的各种方法发出的。
pub struct BlockingClient {
    /// 异步 `Client`。
    inner: crate::clients::Client,

    /// 一个 `current_thread` 运行时，用于以阻塞方式执行异步客户端上的操作。
    rt: Runtime,
}

/// 进入 pub/sub 模式的客户端。
///
/// 一旦客户端订阅了一个频道，它们只能执行与 pub/sub 相关的命令。
/// `BlockingClient` 类型转换为 `BlockingSubscriber` 类型，以防止调用非 pub/sub 方法。
pub struct BlockingSubscriber {
    /// 异步 `Subscriber`。
    inner: crate::clients::Subscriber,

    /// 一个 `current_thread` 运行时，用于以阻塞方式执行异步 `Subscriber` 上的操作。
    rt: Runtime,
}

/// `Subscriber::into_iter` 返回的迭代器。
struct SubscriberIterator {
    /// 异步 `Subscriber`。
    inner: crate::clients::Subscriber,

    /// 一个 `current_thread` 运行时，用于以阻塞方式执行异步 `Subscriber` 上的操作。
    rt: Runtime,
}

impl BlockingClient {
    /// 与位于 `addr` 的 Redis 服务器建立连接。
    ///
    /// `addr` 可以是任何可以异步转换为 `SocketAddr` 的类型。这包括 `SocketAddr` 和字符串。
    /// `ToSocketAddrs` 特性是 Tokio 版本，而不是 `std` 版本。
    ///
    /// # 示例
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let client = match BlockingClient::connect("localhost:6379") {
    ///     Ok(client) => client,
    ///     Err(_) => panic!("failed to establish connection"),
    /// };
    /// # drop(client);
    /// ```
    pub fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<BlockingClient> {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

        let inner = rt.block_on(crate::clients::Client::connect(addr))?;

        Ok(BlockingClient { inner, rt })
    }

    /// 获取键的值。
    ///
    /// 如果键不存在，则返回特殊值 `None`。
    ///
    /// # 示例
    ///
    /// 展示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// let val = client.get("foo").unwrap();
    /// println!("Got = {:?}", val);
    /// ```
    pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
    }

    /// 设置 `key` 以保存给定的 `value`。
    ///
    /// `value` 与 `key` 关联，直到被下一次调用 `set` 覆盖或被删除。
    ///
    /// 如果键已经保存了一个值，则会被覆盖。任何与键关联的先前生存时间在成功的 SET 操作中都会被丢弃。
    ///
    /// # 示例
    ///
    /// 展示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// client.set("foo", "bar".into()).unwrap();
    ///
    /// // 立即获取值
    /// let val = client.get("foo").unwrap().unwrap();
    /// assert_eq!(val, "bar");
    /// ```
    pub fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.rt.block_on(self.inner.set(key, value))
    }

    /// 设置 `key` 以保存给定的 `value`。该值在 `expiration` 后过期。
    ///
    /// `value` 与 `key` 关联，直到以下情况之一：
    /// - 它过期。
    /// - 它被下一次调用 `set` 覆盖。
    /// - 它被删除。
    ///
    /// 如果键已经保存了一个值，则会被覆盖。任何与键关联的先前生存时间在成功的 SET 操作中都会被丢弃。
    ///
    /// # 示例
    ///
    /// 展示基本用法。此示例不 **保证** 始终有效，因为它依赖于基于时间的逻辑，并假设客户端和服务器在时间上保持相对同步。现实世界往往不那么有利。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let ttl = Duration::from_millis(500);
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// client.set_expires("foo", "bar".into(), ttl).unwrap();
    ///
    /// // 立即获取值
    /// let val = client.get("foo").unwrap().unwrap();
    /// assert_eq!(val, "bar");
    ///
    /// // 等待 TTL 过期
    /// thread::sleep(ttl);
    ///
    /// let val = client.get("foo").unwrap();
    /// assert!(val.is_some());
    /// ```
    pub fn set_expires(&mut self, key: &str, value: Bytes, expiration: Duration) -> crate::Result<()> {
        self.rt.block_on(self.inner.set_expires(key, value, expiration))
    }

    /// 将 `message` 发布到给定的 `channel`。
    ///
    /// 返回当前在频道上监听的订阅者数量。不能保证这些订阅者会收到消息，因为他们可能随时断开连接。
    ///
    /// # 示例
    ///
    /// 展示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    /// let val = client.publish("foo", "bar".into()).unwrap();
    /// println!("Got = {:?}", val);
    /// ```
    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }

    /// 订阅客户端到指定的频道。
    ///
    /// 一旦客户端发出订阅命令，它就不能再发出任何非 pub/sub 命令。该函数消耗 `self` 并返回一个 `BlockingSubscriber`。
    ///
    /// `BlockingSubscriber` 值用于接收消息以及管理客户端订阅的频道列表。
    pub fn subscribe(self, channels: Vec<String>) -> crate::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;
        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

impl BlockingSubscriber {
    /// 返回当前订阅的频道集。
    pub fn get_subscribed(&self) -> &[String] {
        self.inner.get_subscribed()
    }

    /// 接收在订阅频道上发布的下一条消息，如果有必要，等待。
    ///
    /// `None` 表示订阅已终止。
    pub fn next_message(&mut self) -> crate::Result<Option<Message>> {
        self.rt.block_on(self.inner.next_message())
    }

    /// 将订阅者转换为一个 `Iterator`，生成在订阅频道上发布的新消息。
    pub fn into_iter(self) -> impl Iterator<Item = crate::Result<Message>> {
        SubscriberIterator {
            inner: self.inner,
            rt: self.rt,
        }
    }

    /// 订阅新的频道列表
    pub fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.subscribe(channels))
    }

    /// 取消订阅新的频道列表
    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.unsubscribe(channels))
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::Result<Message>;

    fn next(&mut self) -> Option<crate::Result<Message>> {
        self.rt.block_on(self.inner.next_message()).transpose()
    }
}
