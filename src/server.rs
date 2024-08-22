//! 最小的 Redis 服务器实现
//!
//! 提供一个异步的 `run` 函数，用于监听入站连接，为每个连接生成一个任务。

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// 服务器监听器状态。在 `run` 调用中创建。它包括一个 `run` 方法
/// 用于执行 TCP 监听和每个连接状态的初始化。
#[derive(Debug)]
struct Server {
    /// 共享数据库句柄。
    ///
    /// 包含键/值存储以及用于发布/订阅的广播通道。
    ///
    /// 这包含一个 `Arc` 的包装器。内部的 `Db` 可以
    /// 被检索并传递到每个连接状态 (`Handler`) 中。
    db_holder: DbDropGuard,
    /// 由 `run` 调用者提供的 TCP 监听器。
    listener: TcpListener,
    /// 限制最大连接数。
    ///
    /// 使用 `Semaphore` 来限制最大连接数。在尝试接受新连接之前，
    /// 从信号量中获取一个许可。如果没有可用的许可，监听器将等待一个。
    ///
    /// 当处理程序完成处理连接时，许可将返回给信号量。
    limit_connections: Arc<Semaphore>,
    /// 向所有活动连接广播关闭信号。
    ///
    /// 初始的 `shutdown` 触发器由 `run` 调用者提供。
    /// 服务器负责优雅地关闭活动连接。
    /// 当生成连接任务时，会传递一个广播接收器句柄。
    /// 当启动优雅关闭时，通过 `broadcast::Sender` 发送一个 `()` 值。
    /// 每个活动连接都会收到它，达到安全的终端状态，并完成任务。
    notify_shutdown: broadcast::Sender<()>,
    /// 用作优雅关闭过程中等待客户端连接完成处理的一部分。
    ///
    /// 一旦所有 `Sender` 句柄超出范围，Tokio 通道将关闭。
    /// 当通道关闭时，接收器会收到 `None`。
    /// 这被用来检测所有连接处理程序的完成。当初始化连接处理程序时，
    /// 它被分配一个 `shutdown_complete_tx` 的克隆。
    /// 当监听器关闭时，它会丢弃由此 `shutdown_complete_tx` 字段持有的发送器。
    /// 一旦所有处理程序任务完成，所有 `Sender` 的克隆也会被丢弃。
    /// 这会导致 `shutdown_complete_rx.recv()` 完成并返回 `None`。
    /// 此时，可以安全地退出服务器进程。
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// 每个连接的处理程序。从 `connection` 读取请求并将命令应用到 `db`。
#[derive(Debug)]
struct Handler {
    /// 共享数据库句柄。
    ///
    /// 当从 `connection` 接收到命令时，它会与 `db` 一起应用。
    /// 命令的实现位于 `cmd` 模块中。每个命令都需要与 `db` 交互以完成工作。
    db: Db,
    /// 用 Redis 协议编码器/解码器装饰的 TCP 连接，
    /// 使用缓冲的 `TcpStream` 实现。
    ///
    /// 当 `Listener` 接收到入站连接时，`TcpStream` 被传递给 `Connection::new`，
    /// 它初始化相关的缓冲区。`Connection` 允许处理程序在“帧”级别操作，
    /// 并将字节级协议解析细节封装在 `Connection` 中。
    connection: Connection,
    /// 监听关闭通知。
    ///
    /// `broadcast::Receiver` 的包装器，与 `Listener` 中的发送器配对。
    /// 连接处理程序处理来自连接的请求，直到对等方断开连接**或**收到关闭通知。
    /// 在后一种情况下，任何正在处理的工作都会继续，直到达到安全状态，
    /// 此时连接终止。
    shutdown: Shutdown,
    /// 不直接使用。相反，当 `Handler` 被丢弃时...？
    _shutdown_complete: mpsc::Sender<()>,
}

/// Redis 服务器将接受的最大并发连接数。
///
/// 当达到此限制时，服务器将停止接受连接，直到一个活动连接终止。
///
/// 实际应用程序可能希望使此值可配置，但在此示例中，它是硬编码的。
///
/// 此值也设置得非常低，以阻止在生产中使用（你可能认为所有免责声明都表明这不是一个严肃的项目……但我对 mini-http 也有同样的想法）。
const MAX_CONNECTIONS: usize = 250;

/// 运行 mini-redis 服务器。
///
/// 接受来自提供的监听器的连接。对于每个入站连接，生成一个任务来处理该连接。
/// 服务器运行直到 `shutdown` future 完成，此时服务器优雅地关闭。
///
/// `tokio::signal::ctrl_c()` 可以用作 `shutdown` 参数。这将监听 SIGINT 信号。
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 当提供的 `shutdown` future 完成时，我们必须向所有活动连接发送关闭消息。
    // 为此，我们使用广播通道。下面的调用忽略了广播对的接收器，当需要接收器时，
    // 使用发送器上的 subscribe() 方法创建一个。
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    // 初始化监听器状态
    let mut server = Server {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };
    // 并发运行服务器并监听 `shutdown` 信号。
    // 服务器任务运行直到遇到错误，因此在正常情况下，
    // 此 `select!` 语句运行直到收到 `shutdown` 信号。
    //
    // `select!` 语句的写法如下：
    //
    // ```
    // <异步操作的结果> = <异步操作> => <使用结果执行的步骤>
    // ```
    //
    // 所有 `<异步操作>` 语句并发执行。一旦**第一个**操作完成，
    // 其关联的 `<使用结果执行的步骤>` 将被执行。
    //
    // `select!` 宏是编写异步 Rust 的基础构建块。有关更多详细信息，请参阅 API 文档：
    //
    // https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // 如果在这里收到错误，接受来自 TCP 监听器的连接失败多次，
            // 服务器放弃并关闭。
            //
            // 处理单个连接时遇到的错误不会冒泡到此点。
            if let Err(err) = res {
                error!(cause = %err, "接受失败");
            }
        }
        _ = shutdown => {
            // 收到关闭信号。
            info!("正在关闭");
        }
    }
    // 显式丢弃 `shutdown_complete` 接收器和发送器
    // 显式丢弃 `shutdown_transmitter`。这很重要，因为下面的 `.await` 否则永远不会完成。
    let Server {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;
    // 当 `notify_shutdown` 被丢弃时，所有 `subscribe` 的任务将
    // 收到关闭信号并可以退出
    drop(notify_shutdown);
    // 丢弃最后一个 `Sender` 以便下面的 `Receiver` 可以完成
    drop(shutdown_complete_tx);

    // 等待所有活动连接完成处理。由于上面监听器持有的 `Sender` 句柄已被丢弃，
    // 唯一剩下的 `Sender` 实例由连接处理程序任务持有。
    // 当这些任务丢弃时，`mpsc` 通道将关闭，`recv()` 将返回 `None`。
    let _ = shutdown_complete_rx.recv().await;
}

impl Server {
    /// 运行服务器
    ///
    /// 监听入站连接。对于每个入站连接，生成一个任务来处理该连接。
    ///
    /// # 错误
    ///
    /// 如果接受返回错误，则返回 `Err`。这可能发生在多种原因下，这些原因会随着时间的推移而解决。
    /// 例如，如果底层操作系统已达到最大套接字数的内部限制，接受将失败。
    ///
    /// 进程无法检测到瞬态错误何时解决。处理此问题的一种策略是实现退避策略，
    /// 这就是我们在这里所做的。
    async fn run(&mut self) -> crate::Result<()> {
        info!("接受入站连接");
        loop {
            // 等待许可可用
            //
            // `acquire_owned` 返回一个绑定到信号量的许可。
            // 当许可值被丢弃时，它会自动返回到信号量。
            //
            // `acquire_owned()` 在信号量关闭时返回 `Err`。我们从不关闭信号量，所以 `unwrap()` 是安全的。
            let permit = self.limit_connections.clone().acquire_owned().await.unwrap();
            // 接受一个新套接字。这将尝试执行错误处理。
            // `accept` 方法内部尝试恢复错误，因此此处的错误是不可恢复的。
            let socket = self.accept().await?;
            // 创建必要的每个连接处理程序状态。
            let mut handler = Handler {
                // 获取共享数据库的句柄。
                db: self.db_holder.db(),
                // 初始化连接状态。这会分配读/写缓冲区以执行 Redis 协议帧解析。
                connection: Connection::new(socket),
                // 接收关闭通知。
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                // 一旦所有克隆被丢弃，通知接收器一半。
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };
            // 生成一个新任务来处理连接。Tokio 任务类似于异步绿色线程，并发执行。
            tokio::spawn(async move {
                // 处理连接。如果遇到错误，记录它。
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "连接错误");
                }
                // 将许可移入任务并在完成后丢弃它。这将许可返回给信号量。
                drop(permit);
            });
        }
    }

    /// 接受入站连接。
    ///
    /// 错误通过退避和重试来处理。使用指数退避策略。
    /// 第一次失败后，任务等待 1 秒。第二次失败后，任务等待 2 秒。
    /// 每次后续失败等待时间加倍。如果在等待 64 秒后第六次尝试接受失败，
    /// 则此函数返回错误。
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        // 尝试接受几次
        loop {
            // 执行接受操作。如果成功接受到套接字，则返回它。否则，保存错误。
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // 接受失败次数过多。返回错误。
                        return Err(err.into());
                    }
                }
            }
            // 暂停执行直到退避期结束。
            time::sleep(Duration::from_secs(backoff)).await;
            // 加倍退避时间
            backoff *= 2;
        }
    }
}

impl Handler {
    /// 处理单个连接。
    ///
    /// 从套接字读取请求帧并处理。响应写回到套接字。
    ///
    /// 目前，未实现流水线。流水线是每个连接并发处理多个请求而不交错帧的能力。
    /// 有关更多详细信息，请参阅：
    /// https://redis.io/topics/pipelining
    ///
    /// 当收到关闭信号时，连接会处理直到达到安全状态，此时它会终止。
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // 只要未收到关闭信号，尝试读取新请求帧。
        while !self.shutdown.is_shutdown() {
            // 在读取请求帧时，也监听关闭信号。
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // 如果收到关闭信号，从 `run` 返回。
                    // 这将导致任务终止。
                    return Ok(());
                }
            };
            // 如果 `read_frame()` 返回 `None`，则对等方关闭了套接字。
            // 没有进一步的工作要做，任务可以终止。
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            // 将 Redis 帧转换为命令结构。如果帧不是有效的 Redis 命令或是不支持的命令，则返回错误。
            let cmd = Command::from_frame(frame)?;
            // 记录 `cmd` 对象。这里的语法是 `tracing` crate 提供的简写。
            // 它可以被认为类似于：
            //
            // ```
            // debug!(cmd = format!("{:?}", cmd));
            // ```
            //
            // `tracing` 提供结构化日志记录，因此信息作为键值对“记录”。
            debug!(?cmd);
            // 执行应用命令所需的工作。这可能会导致数据库状态发生变化。
            //
            // 连接被传递到应用函数中，允许命令直接向连接写入响应帧。
            // 在发布/订阅的情况下，可能会向对等方发送多个帧。
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        }

        Ok(())
    }
}
