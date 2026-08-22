//! 认证与多账户管理模块。
//!
//! 安全模型（防御纵深）：
//!
//! 1. **密码永不明文存储** —— **Argon2id**（内存硬哈希，OWASP 推荐）。不可逆，
//!    64 MiB 内存代价使 GPU/ASIC 暴力破解在经济上不可行。
//! 2. **凭据文件静态加密** —— 整个多账户 JSON 用 **AES-256-GCM** 加密后落盘到
//!    `config/credentials.enc`，主密钥 `config/master.key`（仅服务器可读，0o600）。
//! 3. **会话令牌防篡改** —— 登录后下发 HMAC-SHA256 签名的无状态令牌（HttpOnly Cookie），
//!    含用户名与角色，服务端常量时间校验。
//! 4. **暴力破解防护** —— 按客户端 IP 计失败次数，达到阈值后指数退避锁死。
//! 5. **多账户 + 角色** —— 默认 `admin`（管理员），可新增 `admin` / `user`（普通用户）
//!    两类账户；用户管理接口仅 `admin` 可调用。
//!
//! 初始化：服务器启动时调用 [`init()`]，通过 [`auth_store()`] 全局访问。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use argon2::password_hash::{SaltString, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::{Algorithm, Argon2, Params, Version};
use aes_gcm::aead::Aead;
use aes_gcm::aead::KeyInit;
use aes_gcm::{Aes256Gcm, Key, Nonce};

/// HMAC-SHA256 类型别名
type HmacSha256 = Hmac<Sha256>;

const DEFAULT_USER: &str = "admin";
const DEFAULT_PASSWORD: &str = "admin";
const KEY_LEN: usize = 32;
/// Argon2id 参数（高强度）：64 MiB 内存、3 轮、4 并行度
const ARGON_M: u32 = 65_536;
const ARGON_T: u32 = 3;
const ARGON_P: u32 = 4;
const SESSION_FULL_TTL: u64 = 43_200;
const SESSION_PWCHANGE_TTL: u64 = 900;
const LOCK_THRESHOLD: u32 = 5;
pub const ROLE_ADMIN: &str = "admin";
pub const ROLE_USER: &str = "user";

/// 单个账户
#[derive(Serialize, Deserialize, Clone)]
struct Account {
    username: String,
    /// Argon2id PHC 编码串（含盐与参数）
    phc: String,
    /// "admin" 管理员 | "user" 普通用户
    role: String,
    /// 是否必须修改密码（首次登录/被管理员重置后为 true）
    must_change: bool,
    created_at: u64,
    changed_at: Option<u64>,
}

/// 落盘的多账户存储（会被 AES-256-GCM 加密）
#[derive(Serialize, Deserialize, Clone)]
struct UserStore {
    accounts: Vec<Account>,
}

/// 会话声明（HMAC 签名的载荷）
#[derive(Serialize, Deserialize, Clone)]
pub struct SessionClaims {
    pub sub: String,
    pub role: String,
    pub exp: u64,
    /// "full" 完整会话 | "pwchange" 仅改密会话
    pub scope: String,
}

/// 对外暴露的账户信息（不含密码哈希）
#[derive(Serialize, Clone)]
pub struct AccountInfo {
    pub username: String,
    pub role: String,
    pub must_change: bool,
    pub created_at: u64,
    pub changed_at: Option<u64>,
}

/// 兼容上一版单账户格式的迁移结构
#[derive(Deserialize)]
struct LegacyCredentials {
    username: String,
    phc: String,
    must_change: bool,
    created_at: u64,
    changed_at: Option<u64>,
}

/// 认证存储单例
pub struct AuthStore {
    master: [u8; KEY_LEN],
    store: Mutex<UserStore>,
    creds_path: PathBuf,
    lockout: Mutex<HashMap<String, (u32, u64)>>,
}

impl AuthStore {
    /// 加载或初始化：读取主密钥，加载/创建多账户凭据。
    fn load() -> Result<Arc<Self>, String> {
        let dir = config_dir();
        std::fs::create_dir_all(&dir).map_err(|e| format!("无法创建配置目录 {:?}: {e}", dir))?;

        let master_path = dir.join("master.key");
        let master = load_or_create_master(&master_path)?;

        let creds_path = dir.join("credentials.enc");
        let store = load_or_create_store(&creds_path, &master)?;

        tracing::info!(
            "[auth] 认证模块已初始化，配置目录: {:?}，账户数: {}",
            dir,
            store.accounts.len()
        );

        Ok(Arc::new(Self {
            master,
            store: Mutex::new(store),
            creds_path,
            lockout: Mutex::new(HashMap::new()),
        }))
    }

    /// 校验登录，成功返回角色
    pub fn verify_login(&self, username: &str, password: &str) -> Option<String> {
        let store = self.store.lock().unwrap();
        let account = store.accounts.iter().find(|a| a.username == username)?;
        let Ok(parsed) = PasswordHash::new(&account.phc) else {
            return None;
        };
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
        {
            Some(account.role.clone())
        } else {
            None
        }
    }

    /// 账户是否强制要求改密
    pub fn requires_change(&self, username: &str) -> bool {
        self.store
            .lock()
            .unwrap()
            .accounts
            .iter()
            .find(|a| a.username == username)
            .map(|a| a.must_change)
            .unwrap_or(false)
    }

    /// 账户角色
    pub fn account_role(&self, username: &str) -> Option<String> {
        self.store
            .lock()
            .unwrap()
            .accounts
            .iter()
            .find(|a| a.username == username)
            .map(|a| a.role.clone())
    }

    /// 列出所有账户（不含密码哈希）
    pub fn list_accounts(&self) -> Vec<AccountInfo> {
        self.store
            .lock()
            .unwrap()
            .accounts
            .iter()
            .map(|a| AccountInfo {
                username: a.username.clone(),
                role: a.role.clone(),
                must_change: a.must_change,
                created_at: a.created_at,
                changed_at: a.changed_at,
            })
            .collect()
    }

    /// 新增账户（仅管理员可调用）
    pub fn add_account(
        &self,
        actor_role: &str,
        username: &str,
        password: &str,
        role: &str,
    ) -> Result<(), String> {
        if actor_role != ROLE_ADMIN {
            return Err("权限不足：仅管理员可管理账户".to_string());
        }
        if username.len() < 3 || username.len() > 32 {
            return Err("用户名长度需为 3-32 位".to_string());
        }
        if !username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err("用户名仅允许字母、数字、下划线".to_string());
        }
        if role != ROLE_ADMIN && role != ROLE_USER {
            return Err("角色必须为 admin 或 user".to_string());
        }
        validate_password_policy(password)?;

        let mut store = self.store.lock().unwrap();
        if store.accounts.iter().any(|a| a.username == username) {
            return Err("用户名已存在".to_string());
        }
        let phc = self.hash_password(password)?;
        store.accounts.push(Account {
            username: username.to_string(),
            phc,
            role: role.to_string(),
            must_change: false,
            created_at: now_secs(),
            changed_at: None,
        });
        self.persist(&store)?;
        Ok(())
    }

    /// 删除账户（仅管理员；不能删除自己；至少保留一个管理员）
    pub fn remove_account(&self, actor: &str, username: &str) -> Result<(), String> {
        let mut store = self.store.lock().unwrap();
        if actor == username {
            return Err("不能删除当前登录的账户".to_string());
        }
        let Some(idx) = store.accounts.iter().position(|a| a.username == username) else {
            return Err("账户不存在".to_string());
        };
        if store.accounts[idx].role == ROLE_ADMIN {
            let admin_count = store.accounts.iter().filter(|a| a.role == ROLE_ADMIN).count();
            if admin_count <= 1 {
                return Err("至少需保留一个管理员账户".to_string());
            }
        }
        store.accounts.remove(idx);
        self.persist(&store)?;
        Ok(())
    }

    /// 管理员重置某账户密码（被重置者下次登录需改密）
    pub fn reset_password(&self, actor_role: &str, target: &str, new: &str) -> Result<(), String> {
        if actor_role != ROLE_ADMIN {
            return Err("权限不足：仅管理员可重置密码".to_string());
        }
        validate_password_policy(new)?;
        let mut store = self.store.lock().unwrap();
        let Some(acc) = store.accounts.iter_mut().find(|a| a.username == target) else {
            return Err("账户不存在".to_string());
        };
        acc.phc = self.hash_password(new)?;
        acc.must_change = true;
        acc.changed_at = Some(now_secs());
        self.persist(&store)?;
        Ok(())
    }

    /// 管理员修改某账户角色（admin <-> user），保护“最后一个管理员”不被降级
    pub fn set_role(&self, actor_role: &str, target: &str, new_role: &str) -> Result<(), String> {
        if actor_role != ROLE_ADMIN {
            return Err("权限不足：仅管理员可修改角色".to_string());
        }
        if new_role != ROLE_ADMIN && new_role != ROLE_USER {
            return Err("角色必须为 admin 或 user".to_string());
        }
        let mut store = self.store.lock().unwrap();
        // 先统计管理员数量（不可变借用），避免与下方可变借用冲突
        let admin_count = store.accounts.iter().filter(|a| a.role == ROLE_ADMIN).count();
        let Some(acc) = store.accounts.iter_mut().find(|a| a.username == target) else {
            return Err("账户不存在".to_string());
        };
        if acc.role == ROLE_ADMIN && new_role == ROLE_USER && admin_count <= 1 {
            return Err("至少需保留一个管理员账户".to_string());
        }
        acc.role = new_role.to_string();
        self.persist(&store)?;
        Ok(())
    }

    /// 用户修改自己的密码（需提供当前密码）
    pub fn change_own_password(&self, username: &str, current: &str, new: &str) -> Result<(), String> {
        if !self.verify_login(username, current).is_some() {
            return Err("当前密码错误".to_string());
        }
        if new == current {
            return Err("新密码不能与当前密码相同".to_string());
        }
        validate_password_policy(new)?;
        let mut store = self.store.lock().unwrap();
        let Some(acc) = store.accounts.iter_mut().find(|a| a.username == username) else {
            return Err("账户不存在".to_string());
        };
        acc.phc = self.hash_password(new)?;
        acc.must_change = false;
        acc.changed_at = Some(now_secs());
        self.persist(&store)?;
        Ok(())
    }

    /// 生成 Argon2id 哈希（PHC 编码串）
    fn hash_password(&self, password: &str) -> Result<String, String> {
        let mut salt_bytes = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut salt_bytes);
        let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| e.to_string())?;
        build_argon2()
            .hash_password(password.as_bytes(), &salt)
            .map(|h| h.to_string())
            .map_err(|e| format!("哈希失败: {e}"))
    }

    /// 签发会话令牌（含用户名与角色）
    pub fn sign_session(&self, scope: &str, username: &str, role: &str) -> String {
        let ttl = if scope == "pwchange" {
            SESSION_PWCHANGE_TTL
        } else {
            SESSION_FULL_TTL
        };
        let claims = SessionClaims {
            sub: username.to_string(),
            role: role.to_string(),
            exp: now_secs() + ttl,
            scope: scope.to_string(),
        };
        let json = serde_json::to_vec(&claims).expect("claims 序列化失败");
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.master).expect("HMAC 密钥长度合法");
        mac.update(&json);
        let tag = mac.finalize().into_bytes();
        format!("{}.{}", B64.encode(&json), B64.encode(&tag))
    }

    /// 校验会话令牌，返回声明（常量时间校验 + 过期检查）
    pub fn verify_session(&self, token: &str) -> Option<SessionClaims> {
        let (bjson, btag) = token.split_once('.')?;
        let json = B64.decode(bjson).ok()?;
        let tag = B64.decode(btag).ok()?;
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.master).ok()?;
        mac.update(&json);
        mac.verify_slice(&tag).ok()?;
        let claims: SessionClaims = serde_json::from_slice(&json).ok()?;
        if claims.exp <= now_secs() {
            return None;
        }
        Some(claims)
    }

    /// 从请求头提取并校验会话；`scope` 为所需权限（"full" 或 "pwchange"）。
    pub fn session_from_headers(
        &self,
        headers: &axum::http::HeaderMap,
        scope: &str,
    ) -> Option<SessionClaims> {
        let cookie = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
        for part in cookie.split(';') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("lumino_sid=") {
                let claims = self.verify_session(v)?;
                let ok = match scope {
                    "full" => claims.scope == "full",
                    "pwchange" => claims.scope == "pwchange",
                    other => claims.scope == other,
                };
                return if ok { Some(claims) } else { None };
            }
        }
        None
    }

    // ── 暴力破解防护 ──

    pub fn lock_remaining(&self, key: &str) -> u64 {
        let guard = self.lockout.lock().unwrap();
        if let Some((_, until)) = guard.get(key) {
            let now = now_secs();
            if *until > now {
                return until - now;
            }
        }
        0
    }

    pub fn record_fail(&self, key: &str) {
        let mut guard = self.lockout.lock().unwrap();
        let entry = guard.entry(key.to_string()).or_insert((0, 0));
        entry.0 += 1;
        if entry.0 >= LOCK_THRESHOLD {
            let backoff = 30u64 * 2u64.pow((entry.0 - LOCK_THRESHOLD).min(5));
            entry.1 = now_secs() + backoff.min(3600);
        }
    }

    pub fn record_ok(&self, key: &str) {
        self.lockout.lock().unwrap().remove(key);
    }

    /// 将凭据以 AES-256-GCM 加密后原子写盘（临时文件 + rename）
    fn persist(&self, store: &UserStore) -> Result<(), String> {
        let json = serde_json::to_vec(store).map_err(|e| e.to_string())?;
        let blob = encrypt_blob(&self.master, &json)?;
        let tmp = self.creds_path.with_extension("enc.tmp");
        std::fs::write(&tmp, &blob).map_err(|e| format!("写入临时凭据失败: {e}"))?;
        std::fs::rename(&tmp, &self.creds_path).map_err(|e| format!("替换凭据失败: {e}"))?;
        Ok(())
    }
}

/// 解析配置目录：环境变量 `LUMINO_CONFIG_DIR` 优先，否则 `<CWD>/config`
fn config_dir() -> PathBuf {
    if let Ok(d) = std::env::var("LUMINO_CONFIG_DIR") {
        if !d.is_empty() {
            return PathBuf::from(d);
        }
    }
    PathBuf::from("config")
}

/// 读取或创建主密钥（32 字节随机）
fn load_or_create_master(path: &Path) -> Result<[u8; KEY_LEN], String> {
    if let Ok(text) = std::fs::read_to_string(path) {
        if let Ok(bytes) = B64.decode(text.trim()) {
            if bytes.len() == KEY_LEN {
                let mut key = [0u8; KEY_LEN];
                key.copy_from_slice(&bytes);
                return Ok(key);
            }
        }
    }
    let mut key = [0u8; KEY_LEN];
    rand::rngs::OsRng.fill_bytes(&mut key);
    std::fs::write(path, B64.encode(key))
        .map_err(|e| format!("写入主密钥失败 {:?}: {e}", path))?;
    #[cfg(unix)]
    let _ = std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600));
    Ok(key)
}

/// 读取或创建多账户凭据；缺失时用默认 admin/admin 初始化。兼容旧版单账户格式自动迁移。
fn load_or_create_store(path: &Path, master: &[u8; KEY_LEN]) -> Result<UserStore, String> {
    if let Ok(bytes) = std::fs::read(path) {
        let plain = decrypt_blob(master, &bytes)
            .map_err(|e| format!("凭据解密失败（主密钥不匹配？）: {e}"))?;
        // 新格式
        if let Ok(store) = serde_json::from_slice::<UserStore>(&plain) {
            return Ok(store);
        }
        // 旧版单账户格式 -> 迁移为多账户（默认管理员）
        if let Ok(legacy) = serde_json::from_slice::<LegacyCredentials>(&plain) {
            tracing::warn!("[auth] 检测到旧版单账户凭据，已自动迁移为多账户格式（角色 admin）");
            return Ok(UserStore {
                accounts: vec![Account {
                    username: legacy.username,
                    phc: legacy.phc,
                    role: ROLE_ADMIN.to_string(),
                    must_change: legacy.must_change,
                    created_at: legacy.created_at,
                    changed_at: legacy.changed_at,
                }],
            });
        }
        return Err("凭据解析失败（格式无法识别），请删除 config/ 目录后重试".to_string());
    }
    // 首次启动：默认管理员 admin/admin，强制改密
    let phc = {
        let mut salt_bytes = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut salt_bytes);
        let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| e.to_string())?;
        build_argon2()
            .hash_password(DEFAULT_PASSWORD.as_bytes(), &salt)
            .map_err(|e| format!("默认密码哈希失败: {e}"))?
            .to_string()
    };
    let store = UserStore {
        accounts: vec![Account {
            username: DEFAULT_USER.to_string(),
            phc,
            role: ROLE_ADMIN.to_string(),
            must_change: true,
            created_at: now_secs(),
            changed_at: None,
        }],
    };
    let json = serde_json::to_vec(&store).map_err(|e| e.to_string())?;
    let blob = encrypt_blob(master, &json)?;
    std::fs::write(path, blob).map_err(|e| format!("写入凭据失败 {:?}: {e}", path))?;
    tracing::warn!(
        "[auth] 检测到首次启动，已用默认管理员 '{}' / 密码 '{}' 初始化，请务必在首次登录后立即修改！",
        DEFAULT_USER,
        DEFAULT_PASSWORD
    );
    Ok(store)
}

/// 构造高强度 Argon2id 实例
fn build_argon2() -> Argon2<'static> {
    let params = Params::new(ARGON_M, ARGON_T, ARGON_P, None)
        .expect("Argon2 参数合法（64MiB/3轮/4并行）");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

/// AES-256-GCM 加密：输出 = nonce(12) || ciphertext(含认证标签)
fn encrypt_blob(key: &[u8; KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce_bytes = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| format!("AES 加密失败: {e}"))?;
    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// AES-256-GCM 解密：输入 = nonce(12) || ciphertext
fn decrypt_blob(key: &[u8; KEY_LEN], data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() < 12 {
        return Err("密文过短".to_string());
    }
    let (nonce, ciphertext) = data.split_at(12);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|e| format!("AES 解密失败: {e}"))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ── 全局单例访问 ──

static AUTH: OnceLock<Arc<AuthStore>> = OnceLock::new();

/// 初始化认证模块（服务器启动时调用一次）
pub fn init() -> Result<(), String> {
    let store = AuthStore::load()?;
    AUTH
        .set(store)
        .map_err(|_| "认证模块已被初始化（重复调用）".to_string())
}

/// 获取全局认证存储
pub fn auth_store() -> Arc<AuthStore> {
    AUTH
        .get()
        .expect("[auth] 认证模块未初始化，请先调用 auth::init()")
        .clone()
}

/// 密码强度策略校验
fn validate_password_policy(pw: &str) -> Result<(), String> {
    if pw.len() < 12 {
        return Err("密码长度至少 12 位".to_string());
    }
    if pw.len() > 128 {
        return Err("密码长度不能超过 128 位".to_string());
    }
    let mut classes = 0u32;
    if pw.chars().any(|c| c.is_ascii_lowercase()) {
        classes += 1;
    }
    if pw.chars().any(|c| c.is_ascii_uppercase()) {
        classes += 1;
    }
    if pw.chars().any(|c| c.is_ascii_digit()) {
        classes += 1;
    }
    if pw.chars().any(|c| !c.is_ascii_alphanumeric()) {
        classes += 1;
    }
    if classes < 3 {
        return Err("密码需同时包含：大小写字母、数字、符号 中至少三类".to_string());
    }
    let weak = ["admin", "password", "lumino", "123456", "qwerty", "letmein"];
    if weak.iter().any(|w| pw.to_lowercase().contains(w)) {
        return Err("密码包含常见弱词组，请更换".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_policy_rejects_short() {
        assert!(validate_password_policy("Ab1!").is_err());
    }

    #[test]
    fn password_policy_rejects_weak() {
        assert!(validate_password_policy("Admin123456!").is_err());
    }

    #[test]
    fn password_policy_accepts_strong() {
        assert!(validate_password_policy("K7m#pQ2$vBx9").is_ok());
    }
}
