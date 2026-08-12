//! 工具函数：ID / 邀请码 / 颜色生成
//!
//! 性能设计：全部基于 fastrand（无锁、无系统调用开销），
//! 避免 `uuid` crate 的 ~1MB 依赖体积与熵池开销。

use std::time::{SystemTime, UNIX_EPOCH};

/// 当前毫秒时间戳
#[inline]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 生成 32 位十六进制 ID（与 TS 版 `randomBytes(16).toString('hex')` 等价）
#[inline]
pub fn generate_id() -> String {
    let mut buf = [0u8; 16];
    for b in buf.iter_mut() {
        *b = fastrand::u8(..);
    }
    let mut hex = String::with_capacity(32);
    for b in buf {
        hex.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        hex.push(char::from_digit((b & 0xF) as u32, 16).unwrap());
    }
    hex
}

/// 邀请码字符表（排除易混淆字符，与 TS 版一致）
const INVITE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

/// 生成 6 位邀请码
#[inline]
pub fn generate_invite_code() -> String {
    let mut code = String::with_capacity(6);
    for _ in 0..6 {
        let idx = fastrand::usize(..INVITE_ALPHABET.len());
        code.push(INVITE_ALPHABET[idx] as char);
    }
    code
}

/// 用户配色（12 色循环，与 TS 版一致）
const USER_COLORS: [&str; 12] = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7", "#DDA0DD", "#98D8C8", "#F7DC6F",
    "#BB8FCE", "#85C1E9", "#F8C471", "#82E0AA",
];

/// 为用户分配颜色
#[inline]
pub fn generate_user_color() -> &'static str {
    USER_COLORS[fastrand::usize(..USER_COLORS.len())]
}

/// 校验用户名（非空 / ≤20 字符 / 仅中文·字母·数字·下划线·连字符·空格）
pub fn validate_username(raw: &str) -> Result<String, &'static str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("用户名不能为空");
    }
    if trimmed.chars().count() > 20 {
        return Err("用户名不能超过20个字符");
    }
    if !trimmed
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ' ')
    {
        return Err("用户名包含非法字符");
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_format() {
        let id = generate_id();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn invite_code_format() {
        let code = generate_invite_code();
        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|c| INVITE_ALPHABET.contains(&(c as u8))));
    }

    #[test]
    fn username_validation() {
        assert!(validate_username("  ").is_err());
        assert!(validate_username("").is_err());
        assert!(validate_username(&"a".repeat(21)).is_err());
        assert!(validate_username("正常_用户-1").is_ok());
        assert!(validate_username("bad<tag>").is_err());
    }

    #[test]
    fn color_pool() {
        let c = generate_user_color();
        assert!(USER_COLORS.contains(&c));
    }
}
