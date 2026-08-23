//! 协作房间与项目状态
//!
//! 项目状态 = 音符快照 + 操作日志（delta）：快照保证新成员 fullSync 状态
//! 完整，操作日志供客户端增量重放；日志截断后用快照合成一条 add 兜底。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use super::user::User;
use crate::protocol::{
    NoteAction, NoteBatchOperation, ProjectStateSnapshot, RoomInfo, SyncNote, UserInfo, ViewState,
};
use crate::util::now_ms;

/// 每房间最大协作用户数（与 TS 版一致）
pub const MAX_USERS_PER_ROOM: usize = 10;

/// 房间生命周期策略（控制房间何时被自动回收 / 解散）
///
/// 与「用户在 WS 路径下即时创建瞬时房间」的旧行为兼容：`AfterLeave(0)` 即
/// “最后一名成员离开即关闭”，保持原语义；其余模式把删除时机交给
/// `AppState::lifetime_sweep` 后台轮询处理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomLifetime {
    /// 用户全部离开后 X 分钟关闭；X=0 表示“离开即关闭”（瞬时房间原行为）
    AfterLeave(u64),
    /// 永不自动关闭（仅房主 / 管理员手动解散才关闭）
    Never,
    /// 到达指定绝对时间（epoch ms）后强制解散，无论是否仍有成员在房间内
    Timed(u64),
}

impl Default for RoomLifetime {
    fn default() -> Self {
        RoomLifetime::AfterLeave(0)
    }
}
/// 操作日志上限：超过后丢弃最旧（fullSync 用快照兜底）
pub const MAX_OPS_LOG: usize = 500;

/// 项目状态（每房间一份）
pub struct ProjectState {
    /// 音符快照：note_id → note（供 fullSync 合成 + 操作应用）
    pub(crate) notes: HashMap<u64, SyncNote>,
    /// 操作日志（delta，供客户端重放）
    pub(crate) ops: VecDeque<NoteBatchOperation>,
    /// 操作日志是否被截断过（截断后 fullSync 必须发快照）
    pub(crate) truncated: bool,
    /// 最新视图状态
    pub view_state: ViewState,
    pub last_modified: u64,
    pub modified_by: Option<String>,
    /// paste 操作重新分配 note id 的自增计数器（服务端权威，单调递增）
    pub(crate) next_paste_id: u64,
}

impl ProjectState {
    pub(crate) fn new() -> Self {
        Self {
            notes: HashMap::new(),
            ops: VecDeque::with_capacity(MAX_OPS_LOG),
            truncated: false,
            view_state: ViewState::default(),
            last_modified: now_ms(),
            modified_by: None,
            next_paste_id: 1,
        }
    }

    /// 应用一次音符操作；paste 操作会就地改写 note id（保证全局一致）
    pub fn apply(&mut self, op: &mut NoteBatchOperation, user_id: &str) {
        match op.action {
            NoteAction::Add | NoteAction::Update => {
                for n in &op.notes {
                    self.notes.insert(n.id, n.clone());
                }
            }
            NoteAction::Delete => {
                for n in &op.notes {
                    self.notes.remove(&n.id);
                }
            }
            NoteAction::Move => {
                let tick_off = op.tick_offset.unwrap_or(0.0);
                let key_off = op.key_offset.unwrap_or(0) as i32;
                for n in &op.notes {
                    if let Some(existing) = self.notes.get_mut(&n.id) {
                        existing.tick += tick_off;
                        existing.key = (existing.key as i32 + key_off).max(0) as u16;
                    }
                }
            }
            NoteAction::Copy => { /* 纯客户端操作 */ }
            NoteAction::Paste => {
                let tick_off = op.tick_offset.unwrap_or(0.0);
                let key_off = op.key_offset.unwrap_or(0) as i32;
                let target = op.target_track.unwrap_or(0);
                for n in &mut op.notes {
                    n.id = self.next_paste_id;
                    self.next_paste_id = self.next_paste_id.wrapping_add(1);
                    n.tick += tick_off;
                    n.key = (n.key as i32 + key_off).max(0) as u16;
                    n.track_index = target;
                    self.notes.insert(n.id, n.clone());
                }
            }
        }
        self.ops.push_back(op.clone());
        if self.ops.len() > MAX_OPS_LOG {
            self.ops.pop_front();
            self.truncated = true;
        }
        self.last_modified = now_ms();
        self.modified_by = Some(user_id.to_string());
    }

    /// 生成 fullSync 快照：优先操作日志；被截断过则用完整快照合成一条 add
    pub fn snapshot(&self) -> ProjectStateSnapshot {
        let mut notes = self.ops.iter().cloned().collect::<Vec<_>>();
        if self.truncated {
            let all = self.notes.values().cloned().collect::<Vec<_>>();
            notes = vec![NoteBatchOperation {
                action: NoteAction::Add,
                notes: all,
                source_track: None,
                target_track: None,
                tick_offset: None,
                key_offset: None,
                timestamp: self.last_modified,
            }];
        }
        ProjectStateSnapshot {
            notes,
            view_state: self.view_state.clone(),
            last_modified: self.last_modified,
            modified_by: self.modified_by.clone(),
        }
    }

    /// 更新视图状态（projectUpdate 携带 data 时）
    pub fn set_view_state(&mut self, vs: ViewState, user_id: &str) {
        self.view_state = vs;
        self.last_modified = now_ms();
        self.modified_by = Some(user_id.to_string());
    }
}

/// 协作房间
pub struct Room {
    /// 房间 ID = 邀请码
    pub id: String,
    pub name: String,
    pub host_id: RwLock<Option<String>>,
    pub users: RwLock<HashMap<String, Arc<User>>>,
    pub project: RwLock<ProjectState>,
    pub max_users: usize,
    /// 房主上传的工程名（None 表示尚未上传）
    pub project_name: RwLock<Option<String>>,
    /// 房主工程文件的内容哈希（None 表示尚未上传）
    pub project_hash: RwLock<Option<String>>,
    /// 房间生命周期策略（控制自动回收 / 解散时机）
    pub(crate) lifetime: RwLock<RoomLifetime>,
    /// 房间变为空（最后一名成员离开）的时间戳；用于“离开后延迟关闭”计时
    pub(crate) empty_since: RwLock<Option<u64>>,
}

impl Room {
    pub(crate) fn new(id: String, name: String, host: Option<&Arc<User>>) -> Arc<Self> {
        let room = Arc::new(Self {
            id,
            name,
            host_id: RwLock::new(host.map(|u| u.id.clone())),
            users: RwLock::new(HashMap::new()),
            project: RwLock::new(ProjectState::new()),
            max_users: MAX_USERS_PER_ROOM,
            project_name: RwLock::new(None),
            project_hash: RwLock::new(None),
            lifetime: RwLock::new(RoomLifetime::AfterLeave(0)),
            empty_since: RwLock::new(None),
        });
        if let Some(host) = host {
            room.add_user(host);
        }
        room
    }

    pub fn add_user(&self, user: &Arc<User>) {
        self.users
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(user.id.clone(), Arc::clone(user));
    }

    pub fn remove_user(&self, user_id: &str) -> Option<Arc<User>> {
        self.users
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(user_id)
    }

    pub fn user_count(&self) -> usize {
        self.users.read().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// 房间内用户信息列表（含 isHost 标记）
    pub fn users_info(&self) -> Vec<UserInfo> {
        let users = self.users.read().unwrap_or_else(|p| p.into_inner());
        let host = self
            .host_id
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        users
            .values()
            .map(|u| UserInfo {
                id: u.id.clone(),
                username: u.username.clone(),
                color: u.color,
                is_host: Some(&u.id) == host.as_ref(),
            })
            .collect()
    }

    pub fn info(&self) -> RoomInfo {
        let meta = |lock: &RwLock<Option<String>>| {
            lock.read()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .unwrap_or_default()
        };
        RoomInfo {
            id: self.id.clone(),
            invite_code: self.id.clone(),
            name: self.name.clone(),
            host_id: self
                .host_id
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .unwrap_or_default(),
            user_count: self.user_count(),
            max_users: self.max_users,
            project_name: meta(&self.project_name),
            project_hash: meta(&self.project_hash),
        }
    }

    /// 记录房主上传的工程名与内容哈希（供加入者判断“是否同一份工程”）
    pub fn set_project_meta(&self, name: Option<String>, hash: Option<String>) {
        *self
            .project_name
            .write()
            .unwrap_or_else(|p| p.into_inner()) = name;
        *self
            .project_hash
            .write()
            .unwrap_or_else(|p| p.into_inner()) = hash;
    }

    // ── 生命周期策略访问器（供 AppState 调度删除时机）──

    /// 设置房间生命周期策略（预创建 / 管理后台调用）
    pub(crate) fn set_lifetime(&self, lifetime: RoomLifetime) {
        *self
            .lifetime
            .write()
            .unwrap_or_else(|p| p.into_inner()) = lifetime;
    }

    /// 读取当前生命周期策略（Copy 类型，直接返回）
    pub(crate) fn lifetime(&self) -> RoomLifetime {
        *self.lifetime.read().unwrap_or_else(|p| p.into_inner())
    }

    /// 房间被占用（有成员加入）时清空空置计时
    pub(crate) fn mark_occupied(&self) {
        *self
            .empty_since
            .write()
            .unwrap_or_else(|p| p.into_inner()) = None;
    }

    /// 房间变空时记录空置起点（仅在尚未记录时写入，避免重复刷新计时）
    pub(crate) fn mark_emptied(&self) {
        let mut g = self
            .empty_since
            .write()
            .unwrap_or_else(|p| p.into_inner());
        if g.is_none() {
            *g = Some(now_ms());
        }
    }

    /// 读取空置起点（None 表示从未空置 / 当前有人）
    pub(crate) fn empty_since(&self) -> Option<u64> {
        *self
            .empty_since
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }
}
