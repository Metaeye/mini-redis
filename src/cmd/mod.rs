mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod del;
pub use del::Del;

mod publish;
pub use publish::Publish;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parser, ParserError, Shutdown};

/// 支持的 Redis 命令的枚举。
///
/// 在 `Command` 上调用的方法会委托给命令实现。
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Del(Del),
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    /// 将命令应用于指定的 `Db` 实例。
    ///
    /// 响应写入 `dst`。这是由服务器调用以执行接收到的命令。
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection, shutdown: &mut Shutdown) -> crate::Result<()> {
        match self {
            Self::Get(cmd) => cmd.apply(db, dst).await,
            Self::Set(cmd) => cmd.apply(db, dst).await,
            Self::Del(cmd) => cmd.apply(db, dst).await,
            Self::Publish(cmd) => cmd.apply(db, dst).await,
            Self::Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Self::Ping(cmd) => cmd.apply(dst).await,
            Self::Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` 不能被应用。它只能在 `Subscribe` 命令的上下文中接收。
            Self::Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }

    /// 返回命令名称
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Self::Get(_) => "get",
            Self::Set(_) => "set",
            Self::Del(_) => "del",
            Self::Publish(_) => "pub",
            Self::Subscribe(_) => "subscribe",
            Self::Unsubscribe(_) => "unsubscribe",
            Self::Ping(_) => "ping",
            Self::Unknown(cmd) => cmd.get_name(),
        }
    }
}

/// 从接收到的帧中解析命令。
///
/// `Frame` 必须表示 `mini-redis` 支持的 Redis 命令，并且是数组变体。
///
/// # 返回值
///
/// 成功时返回命令值，否则返回 `Err`。
impl TryFrom<Frame> for Command {
    type Error = crate::Error;
    fn try_from(frame: Frame) -> crate::Result<Self> {
        // 帧值用 `Parse` 装饰。`Parse` 提供了一个类似“游标”的 API，使解析命令更容易。
        //
        // 帧值必须是数组变体。任何其他帧变体都会导致返回错误。
        let mut parser = Parser::new(frame)?;
        // 所有 Redis 命令都以命令名称作为字符串开头。读取名称并转换为小写以进行区分大小写的匹配。
        let cmd_name = parser.next_string()?.to_lowercase();
        // 匹配命令名称，将其余的解析委托给特定命令。
        let cmd = match &cmd_name[..] {
            "get" => Self::Get(Get::try_from(&mut parser)?),
            "set" => Self::Set(Set::try_from(&mut parser)?),
            "del" => Self::Del(Del::try_from(&mut parser)?),
            "publish" => Self::Publish(Publish::try_from(&mut parser)?),
            "subscribe" => Self::Subscribe(Subscribe::try_from(&mut parser)?),
            "unsubscribe" => Self::Unsubscribe(Unsubscribe::try_from(&mut parser)?),
            "ping" => Self::Ping(Ping::try_from(&mut parser)?),
            _ => {
                // 命令未被识别，返回 Unknown 命令。
                //
                // 这里调用 `return` 以跳过下面的 `finish()` 调用。由于命令未被识别，`Parse` 实例中很可能还有未消费的字段。
                return Ok(Self::Unknown(Unknown::new(cmd_name)));
            }
        };
        // 检查 `Parse` 值中是否有任何未消费的字段。如果有剩余字段，这表示帧格式意外，返回错误。
        parser.finish()?;

        // 命令已成功解析
        Ok(cmd)
    }
}
