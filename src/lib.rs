mod frame;
pub use frame::{Frame, FrameError};

mod parser;
use parser::{Parser, ParserError};

pub mod cmd;
pub use cmd::Command;

mod connection;
pub use connection::Connection;

mod db;
use db::{Db, DbDropGuard};

pub mod clients;
pub use clients::{BlockingClient, BufferedClient, Client};

pub mod server;

mod shutdown;
use shutdown::Shutdown;
/// Redis 服务器监听的默认端口。
///
/// 如果未指定端口，则使用此端口。
pub const DEFAULT_PORT: u16 = 6379;

/// 大多数函数返回的错误。
///
/// 在编写实际应用程序时，可能需要考虑使用专门的错误处理 crate 或将错误类型定义为原因的 `enum`。但是，对于我们的示例，使用装箱的 `std::error::Error` 就足够了。
///
/// 出于性能原因，在任何热点路径中都避免装箱。例如，在 `parse` 中，定义了一个自定义错误 `enum`。这是因为在正常执行期间，当在套接字上接收到部分帧时，会遇到并处理该错误。`std::error::Error` 为 `parse::Error` 实现，这允许它转换为 `Box<dyn std::error::Error>`。
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// mini-redis 操作的专用 `Result` 类型。
///
/// 这是为了方便定义的。
pub type Result<T> = std::result::Result<T, Error>;
