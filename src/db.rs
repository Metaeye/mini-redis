use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// `Db` 实例的包装器。此结构体存在的目的是在此结构体被丢弃时，通过通知后台清理任务关闭来有序地清理 `Db`。
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// 当此 `DbDropGuard` 结构体被丢弃时，将关闭的 `Db` 实例。
    db: Db,
}

/// 所有连接共享的服务器状态。
///
/// `Db` 包含一个 `HashMap`，用于存储键/值数据和所有活动的 pub/sub 频道的 `broadcast::Sender` 值。
///
/// `Db` 实例是共享状态的句柄。克隆 `Db` 是浅拷贝，只会增加一个原子引用计数。
///
/// 当创建 `Db` 值时，会生成一个后台任务。此任务用于在请求的持续时间过后过期值。任务运行直到所有 `Db` 实例被丢弃，此时任务终止。
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// 共享状态的句柄。后台任务也将拥有一个 `Arc<Shared>`。
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// 共享状态由互斥锁保护。这是一个 `std::sync::Mutex`，而不是 Tokio 互斥锁。
    /// 这是因为在持有互斥锁时没有执行异步操作。此外，临界区非常小。
    ///
    /// Tokio 互斥锁主要用于需要在 `.await` 让步点持有锁的情况。所有其他情况通常最好使用 std 互斥锁。
    /// 如果临界区不包括任何异步操作但很长（CPU 密集型或执行阻塞操作），则整个操作，包括等待互斥锁，都会被视为“阻塞”操作，
    /// 应使用 `tokio::task::spawn_blocking`。
    state: Mutex<State>,
    /// 通知处理条目过期的后台任务。后台任务等待此通知，然后检查过期值或关闭信号。
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// 键值数据。我们不打算做任何花哨的事情，所以 `std::collections::HashMap` 就可以了。
    entries: HashMap<String, Entry>,
    /// pub/sub 键空间。Redis 使用一个**单独的**键空间来存储键值和 pub/sub。
    /// `mini-redis` 通过使用一个单独的 `HashMap` 来处理这个问题。
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    /// 跟踪键的 TTL。
    ///
    /// 使用 `BTreeSet` 来维护按过期时间排序的过期条目。这允许后台任务迭代此映射以找到下一个过期的值。
    ///
    /// 虽然极不可能，但有可能在同一时刻创建多个过期条目。
    /// 因此，`Instant` 对于键来说是不够的。使用唯一键（`String`）来解决这些冲突。
    expirations: BTreeSet<(Instant, String)>,
    /// 当 Db 实例正在关闭时为 true。当所有 `Db` 值都丢弃时会发生这种情况。
    /// 将此设置为 `true` 会向后台任务发出退出信号。
    is_shutdown: bool,
}

/// 键值存储中的条目
#[derive(Debug)]
struct Entry {
    /// 存储的数据
    data: Bytes,
    /// 条目过期并应从数据库中删除的时间点。
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// 创建一个新的 `DbDropGuard`，包装一个 `Db` 实例。当此实例被丢弃时，`Db` 的清理任务将被关闭。
    pub(crate) fn new() -> Self {
        Self { db: Db::new() }
    }

    /// 获取共享数据库。在内部，这是一个 `Arc`，所以克隆只会增加引用计数。
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 向 'Db' 实例发出信号，关闭清理过期键的任务
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// 创建一个新的、空的 `Db` 实例。分配共享状态并生成一个后台任务来管理键过期。
    pub(crate) fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                is_shutdown: false,
            }),
            background_task: Notify::new(),
        });
        // 启动后台任务。
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Self { shared }
    }

    /// 获取与键关联的值。
    ///
    /// 如果没有与键关联的值，则返回 `None`。这可能是因为从未为键分配过值，或者先前分配的值已过期。
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 获取锁，获取条目并克隆值。
        //
        // 因为数据是使用 `Bytes` 存储的，所以这里的克隆是浅克隆。数据不会被复制。
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 设置与键关联的值以及可选的过期持续时间。
    ///
    /// 如果已经有值与键关联，则将其删除。
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        // 如果此 `set` 成为**下一个**过期的键，则需要通知后台任务以便它可以更新其状态。
        //
        // 是否需要通知任务是在 `set` 例程中计算的。
        let mut notify = false;
        let expires_at = expire.map(|duration| {
            // 键过期的 `Instant`。
            let when = Instant::now() + duration;

            // 仅当新插入的过期时间是下一个要驱逐的键时才通知工作任务。在这种情况下，需要唤醒工作任务以更新其状态。
            notify = state.next_expiration().map(|expiration| expiration > when).unwrap_or(true);

            when
        });
        // 将条目插入 `HashMap`。
        let prev = state.entries.insert(key.clone(), Entry { data: value, expires_at });
        // 如果先前有值与键关联**并且**它有过期时间。必须删除 `expirations` 映射中的关联条目。这可以避免数据泄漏。
        if let Some(entry) = prev {
            if let Some(when) = entry.expires_at {
                // 清除过期时间
                state.expirations.remove(&(when, key.clone()));
            }
        }
        // 跟踪过期时间。如果我们在删除之前插入，当当前 `(when, key)` 等于之前的 `(when, key)` 时会导致错误。
        // 先删除再插入可以避免这种情况。
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }
        // 在通知后台任务之前释放互斥锁。这有助于减少争用，避免后台任务唤醒后无法获取互斥锁，因为此函数仍在持有它。
        drop(state);

        if notify {
            // 最后，仅当后台任务需要更新其状态以反映新的过期时间时才通知它。
            self.shared.background_task.notify_one();
        }
    }

    /// 返回请求频道的 `Receiver`。
    ///
    /// 返回的 `Receiver` 用于接收 `PUBLISH` 命令广播的值。
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 获取互斥锁
        let mut state = self.shared.state.lock().unwrap();

        // 如果请求频道没有条目，则创建一个新的广播频道并将其与键关联。如果已经存在，则返回一个关联的接收器。
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // 尚不存在广播频道，因此创建一个。
                //
                // 频道的容量为 `1024` 条消息。消息存储在频道中，直到**所有**订阅者都看到它。
                // 这意味着慢速订阅者可能会导致消息无限期地保留。
                //
                // 当频道的容量已满时，发布将导致旧消息被丢弃。这可以防止慢速消费者阻塞整个系统。
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// 向频道发布消息。返回正在监听频道的订阅者数量。
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // 成功在广播频道上发送消息时，返回订阅者数量。错误表示没有接收者，在这种情况下，应返回 `0`。
            .map(|tx| tx.send(value).unwrap_or(0))
            // 如果频道键没有条目，则没有订阅者。在这种情况下，返回 `0`。
            .unwrap()
    }

    /// 向清理后台任务发出关闭信号。这是由 `DbShutdown` 的 `Drop` 实现调用的。
    fn shutdown_purge_task(&self) {
        // 必须向后台任务发出关闭信号。这是通过将 `State::shutdown` 设置为 `true` 并通知任务来完成的。
        let mut state = self.shared.state.lock().unwrap();
        state.is_shutdown = true;
        // 在通知后台任务之前释放锁。这有助于减少锁争用，确保后台任务唤醒后不会因为无法获取互斥锁而无法执行。
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// 清除所有过期键并返回**下一个**键将过期的 `Instant`。后台任务将睡眠直到此时刻。
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.is_shutdown {
            // 数据库正在关闭。所有共享状态的句柄都已丢弃。后台任务应退出。
            return None;
        }
        // 这是为了让借用检查器满意。简而言之，`lock()` 返回一个 `MutexGuard` 而不是 `&mut State`。
        // 借用检查器无法“透过”互斥锁守卫确定同时访问 `state.expirations` 和 `state.entries` 是安全的，
        // 因此我们在循环外获取 `State` 的“真实”可变引用。
        let state = &mut *state;
        // 查找所有计划在现在之前过期的键。
        let now = Instant::now();
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // 完成清除，`when` 是下一个键过期的时间点。工作任务将等待直到此时刻。
                return Some(when);
            }

            // 键已过期，删除它
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// 返回 `true` 如果数据库正在关闭
    ///
    /// 当所有 `Db` 值都已丢弃时，设置 `shutdown` 标志，表示共享状态不再可访问。
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().is_shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations.iter().next().map(|expiration| expiration.0)
    }
}

/// 由后台任务执行的例程。
///
/// 等待通知。收到通知后，从共享状态句柄中清除任何过期的键。如果设置了 `shutdown`，则终止任务。
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 如果设置了关闭标志，则任务应退出。
    while !shared.is_shutdown() {
        // 清除所有过期的键。函数返回下一个键将过期的时间点。工作任务应等待直到此时刻，然后再次清除。
        if let Some(when) = shared.purge_expired_keys() {
            // 等待直到下一个键过期**或**直到后台任务被通知。如果任务被通知，则必须重新加载其状态，因为新键已设置为提前过期。这是通过循环完成的。
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 将来没有键过期。等待任务被通知。
            shared.background_task.notified().await;
        }
    }

    debug!("清理后台任务已关闭")
}
