//! 认证与凭据安全存储模块。
//!
//! 安全模型（防御纵深，The Algorithm 的"质疑+简化"步）：
//!
//! 1. **密码永不明文存储** —— 使用 **Argon2id**（PHC 竞赛胜出算法、OWASP 推荐）
//!    做内存硬哈希。哈希不可逆，即使存储层被攻破也无法还原密码；64 MiB 内存
//!    代价使 GPU/ASIC 暴力破解在经济上不可行。
//! 2. **凭据文件静态加密** —— 整个凭据 JSON 用 **AES-256-GCM**（带认证标签）加密后
//!    落盘到 `config/credentials.enc`。即便文件被拷贝，没有主密钥 `config/master.key`
//!    仍无法读取；GCM 的认证标签还能检测任何篡改。
//! 3. **主密钥隔离** —— 32 字节随机主密钥存于 `config/master.key`（仅服务器启动时可写，
//!    权限 0o600），与凭据文件分离，构成两道独立防线。
//! 4. **会话令牌防篡改** —— 登录后下发 HMAC-SHA256 签名的无状态令牌（存 HttpOnly
//!    Cookie），服务端常量时间校验，杜绝伪造/篡改。
//! 5. **暴力破解防护** —— 按客户端 IP 计失败次数，达到阈值后指数退避锁死。
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

/// 默认管理员用户名
const DEFAULT_USER: &str = "admin";
/// 默认初始密码（首次登录后强制修改）
const DEFAULT_PASSWORD: &str = "admin";
/// 主密钥长度（AES-256 = 32 字节）
const KEY_LEN: usize = 32;
/// Argon2id 参数（高强度）：64 MiB 内存、3 轮、4 并行度
const ARGON_M: u32 = 65_536; // KiB → 64 MiB
const ARGON_T: u32 = 3;
const ARGON_P: u32 = 4;
/// 完整会话有效期（秒）：12 小时
const SESSION_FULL_TTL: u64 = 43_200;
/// 改密会话有效期（秒）：15 分钟（仅允许访问 /change-password）
const SESSION_PWCHANGE_TTL: u64 = 900;
/// 触发锁定的失败次数
const LOCK_THRESHOLD: u32 = 5;

/// 落盘的凭据结构（会被 AES-256-GCM 加密）
#[derive(Serialize, Deserialize, Clone)]
struct Credentials {
    username: String,
    /// Argon2id PHC 编码串（含盐与参数），例如 `$argon2id$v=19$m=...$...$...`
    phc: String,
    /// 是否必须修改密码（首次登录为 true）
    must_change: bool,
    created_at: u64,
    changed_at: Option<u64>,
}

/// 会话声明（HMAC 签名的载荷）
#[derive(Serialize, Deserialize, Clone)]
pub struct SessionClaims {
    pub sub: String,
    pub exp: u64,
    /// "full" 完整会话 | "pwchange" 仅改密会话
    pub scope: String,
}

/// 认证存储单例
pub struct AuthStore {
    /// 主加密密钥（AES-256-GCM + HMAC 共用）
    master: [u8; KEY_LEN],
    /// 当前凭据（受互斥锁保护，改密时写回）
    creds: Mutex<Credentials>,
    /// 凭据加密文件路径
    creds_path: PathBuf,
    /// 暴力破解防护：IP -> (失败次数, 锁死到的时间戳)
    lockout: Mutex<HashMap<String, (u32, u64)>>,
}

impl AuthStore {
    /// 加载或初始化：读取主密钥与凭据，缺失则生成默认值。
    fn load() -> Result<Arc<Self>, String> {
        let dir = config_dir();
        std::fs::create_dir_all(&dir).map_err(|e| format!("无法创建配置目录 {:?}: {e}", dir))?;

        let master_path = dir.join("master.key");
        let master = load_or_create_master(&master_path)?;

        let creds_path = dir.join("credentials.enc");
        let creds = load_or_create_creds(&creds_path, &master)?;

        tracing::info!(
            "[auth] 认证模块已初始化，配置目录: {:?}，强制改密: {}",
            dir,
            creds.must_change
        );

        Ok(Arc::new(Self {
            master,
            creds: Mutex::new(creds),
            creds_path,
            lockout: Mutex::new(HashMap::new()),
        }))
    }

    /// 校验密码是否正确
    pub fn verify_password(&self, password: &str) -> bool {
        let creds = self.creds.lock().unwrap();
        let Ok(parsed) = PasswordHash::new(&creds.phc) else {
            return false;
        };
        // 使用哈希内自带的参数做验证（Argon2::default() 即 Argon2id）
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    }

    /// 是否强制要求修改密码
    pub fn requires_change(&self) -> bool {
        self.creds.lock().unwrap().must_change
    }

    /// 修改密码：校验当前密码 + 强度策略，通过后以 Argon2id 重新哈希并静态加密落盘。
    pub fn change_password(&self, current: &str, new: &str) -> Result<(), String> {
        if !self.verify_password(current) {
            return Err("当前密码错误".to_string());
        }
        if new == current {
            return Err("新密码不能与当前密码相同".to_string());
        }
        validate_password_policy(new)?;

        // 生成新盐并哈希（高强度 Argon2id 参数）
        let mut salt_bytes = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut salt_bytes);
        let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| e.to_string())?;
        let argon2 = build_argon2();
        let phc = argon2
            .hash_password(new.as_bytes(), &salt)
            .map_err(|e| format!("哈希失败: {e}"))?
            .to_string();

        let mut creds = self.creds.lock().unwrap();
        creds.phc = phc;
        creds.must_change = false;
        creds.changed_at = Some(now_secs());
        self.persist(&creds)?;
        Ok(())
    }

    /// 签发会话令牌（HMAC-SHA256 签名）
    pub fn sign_session(&self, scope: &str) -> String {
        let ttl = if scope == "pwchange" {
            SESSION_PWCHANGE_TTL
        } else {
            SESSION_FULL_TTL
        };
        let claims = SessionClaims {
            sub: DEFAULT_USER.to_string(),
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
        // verify_slice 为常量时间比较，抗时序攻击
        mac.verify_slice(&tag).ok()?;
        let claims: SessionClaims = serde_json::from_slice(&json).ok()?;
        if claims.exp <= now_secs() {
            return None;
        }
        Some(claims)
    }

    /// 从请求头提取并校验会话；`scope` 为所需权限（"full" 或 "pwchange"）。
    /// "pwchange" 令牌无权访问 "full" 资源。
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

    /// 返回该客户端的剩余锁死秒数（0 表示未锁）
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

    /// 记录一次失败尝试，达到阈值后指数退避锁死（上限 1 小时）
    pub fn record_fail(&self, key: &str) {
        let mut guard = self.lockout.lock().unwrap();
        let entry = guard.entry(key.to_string()).or_insert((0, 0));
        entry.0 += 1;
        if entry.0 >= LOCK_THRESHOLD {
            let backoff = 30u64 * 2u64.pow((entry.0 - LOCK_THRESHOLD).min(5));
            entry.1 = now_secs() + backoff.min(3600);
        }
    }

    /// 成功后清除失败计数
    pub fn record_ok(&self, key: &str) {
        self.lockout.lock().unwrap().remove(key);
    }

    /// 将凭据以 AES-256-GCM 加密后原子写盘（临时文件 + rename）
    fn persist(&self, creds: &Credentials) -> Result<(), String> {
        let json = serde_json::to_vec(creds).map_err(|e| e.to_string())?;
        let blob = encrypt_blob(&self.master, &json)?;
        let tmp = self.creds_path.with_extension("enc.tmp");
        std::fs::write(&tmp, &blob).map_err(|e| format!("写入临时凭据失败: {e}"))?;
        std::fs::rename(&tmp, &self.creds_path).map_err(|e| format!("替换凭据失败: {e}"))?;
        Ok(())
    }
}

/// 解析配置目录：环境变量 `LUMINO_CONFIG_DIR` 优先，否则使用 `<CWD>/config`。
fn config_dir() -> PathBuf {
    if let Ok(d) = std::env::var("LUMINO_CONFIG_DIR") {
        if !d.is_empty() {
            return PathBuf::from(d);
        }
    }
    PathBuf::from("config")
}

/// 读取或创建主密钥（32 字节随机），以 base64 存于 `master.key`。
fn load_or_create_master(path: &Path) -> Result<[u8; KEY_LEN], String> {
    if let Ok(text) = std::fs::read_to_string(path) {
        let text = text.trim();
        if let Ok(bytes) = B64.decode(text) {
            if bytes.len() == KEY_LEN {
                let mut key = [0u8; KEY_LEN];
                key.copy_from_slice(&bytes);
                return Ok(key);
            }
        }
    }
    // 生成新主密钥
    let mut key = [0u8; KEY_LEN];
    rand::rngs::OsRng.fill_bytes(&mut key);
    std::fs::write(path, B64.encode(key))
        .map_err(|e| format!("写入主密钥失败 {:?}: {e}", path))?;
    // 尽量收窄文件权限（仅 Unix 生效；Windows 忽略该调用）
    #[cfg(unix)]
    let _ = std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600));
    Ok(key)
}

/// 读取或创建凭据文件。缺失时用默认密码 "admin" 哈希并标记强制改密。
fn load_or_create_creds(path: &Path, master: &[u8; KEY_LEN]) -> Result<Credentials, String> {
    if let Ok(bytes) = std::fs::read(path) {
        let plain = decrypt_blob(master, &bytes)
            .map_err(|e| format!("凭据解密失败（主密钥不匹配？）: {e}"))?;
        return serde_json::from_slice(&plain)
            .map_err(|e| format!("凭据解析失败: {e}"));
    }
    // 首次启动：用默认密码生成高强度哈希
    let mut salt_bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| e.to_string())?;
    let argon2 = build_argon2();
    let phc = argon2
        .hash_password(DEFAULT_PASSWORD.as_bytes(), &salt)
        .map_err(|e| format!("默认密码哈希失败: {e}"))?
        .to_string();
    let creds = Credentials {
        username: DEFAULT_USER.to_string(),
        phc,
        must_change: true,
        created_at: now_secs(),
        changed_at: None,
    };
    // 立即落盘（加密）
    let json = serde_json::to_vec(&creds).map_err(|e| e.to_string())?;
    let blob = encrypt_blob(master, &json)?;
    std::fs::write(path, blob).map_err(|e| format!("写入凭据失败 {:?}: {e}", path))?;
    tracing::warn!(
        "[auth] 检测到首次启动，已用默认密码 '{}' 初始化，请务必在首次登录后立即修改！",
        DEFAULT_PASSWORD
    );
    Ok(creds)
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

/// 当前秒级时间戳
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
    AUTH.set(store)
        .map_err(|_| "认证模块已被初始化（重复调用）".to_string())
}

/// 获取全局认证存储
pub fn auth_store() -> Arc<AuthStore> {
    AUTH
        .get()
        .expect("[auth] 认证模块未初始化，请先调用 auth::init()")
        .clone()
}

/// 密码强度策略校验，返回具体错误原因
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
        assert!(validate_password_policy("Admin123456!").is_err()); // 含 "admin"
    }

    #[test]
    fn password_policy_accepts_strong() {
        assert!(validate_password_policy("K7m#pQ2$vBx9").is_ok());
    }
}
