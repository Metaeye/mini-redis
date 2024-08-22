use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// 用于解析命令的工具
///
/// 命令表示为数组帧。帧中的每个条目都是一个“令牌”。
/// `Parse` 使用数组帧初始化，并提供类似光标的 API。
/// 每个命令结构体都包含一个 `parse_frame` 方法，该方法使用 `Parse` 来提取其字段。
#[derive(Debug)]
pub(crate) struct Parser {
    /// 数组帧迭代器。
    parts: vec::IntoIter<Frame>,
}

/// 解析帧时遇到的错误。
///
/// 只有 `EndOfStream` 错误在运行时处理。所有其他错误都会导致连接终止。
#[derive(Debug)]
pub(crate) enum ParserError {
    /// 由于帧已完全消耗，尝试提取值失败。
    EndOfStream,
    /// 所有其他错误
    Other(crate::Error),
}

impl Parser {
    /// 创建一个新的 `Parse` 来解析 `frame` 的内容。
    ///
    /// 如果 `frame` 不是数组帧，则返回 `Err`。
    pub(crate) fn new(frame: Frame) -> Result<Self, ParserError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("协议错误；预期数组，得到 {:?}", frame).into()),
        };

        Ok(Self {
            parts: array.into_iter(),
        })
    }

    /// 返回下一个条目。数组帧是帧的数组，因此下一个条目是一个帧。
    fn next(&mut self) -> Result<Frame, ParserError> {
        self.parts.next().ok_or(ParserError::EndOfStream)
    }

    /// 将下一个条目作为字符串返回。
    ///
    /// 如果下一个条目不能表示为字符串，则返回错误。
    pub(crate) fn next_string(&mut self) -> Result<String, ParserError> {
        match self.next()? {
            // `Simple` 和 `Bulk` 表示都可以是字符串。字符串被解析为 UTF-8。
            //
            // 虽然错误存储为字符串，但它们被视为单独的类型。
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(bytes) => String::from_utf8(bytes.to_vec()).map_err(|_| "协议错误；无效字符串".into()),
            frame => Err(format!("协议错误；预期简单帧或批量帧，得到 {:?}", frame).into()),
        }
    }

    /// 将下一个条目作为原始字节返回。
    ///
    /// 如果下一个条目不能表示为原始字节，则返回错误。
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParserError> {
        match self.next()? {
            // `Simple` 和 `Bulk` 表示都可以是原始字节。
            //
            // 虽然错误存储为字符串，可以表示为原始字节，但它们被视为单独的类型。
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!("协议错误；预期简单帧或批量帧，得到 {:?}", frame).into()),
        }
    }

    /// 将下一个条目作为整数返回。
    ///
    /// 这包括 `Simple`、`Bulk` 和 `Integer` 帧类型。`Simple` 和 `Bulk` 帧类型被解析。
    ///
    /// 如果下一个条目不能表示为整数，则返回错误。
    pub(crate) fn next_int(&mut self) -> Result<u64, ParserError> {
        use atoi::atoi;

        const MSG: &str = "协议错误；无效数字";

        match self.next()? {
            // 整数帧类型已存储为整数。
            Frame::Integer(v) => Ok(v),
            // 简单和批量帧必须解析为整数。如果解析失败，则返回错误。
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("协议错误；预期整数帧，但得到 {:?}", frame).into()),
        }
    }

    /// 确保数组中没有更多条目
    pub(crate) fn finish(&mut self) -> Result<(), ParserError> {
        self.parts
            .next()
            .map_or(Ok(()), |_| Err("协议错误；预期帧结束，但还有更多".into()))
    }
}

impl From<String> for ParserError {
    fn from(src: String) -> ParserError {
        ParserError::Other(src.into())
    }
}

impl From<&str> for ParserError {
    fn from(src: &str) -> ParserError {
        src.to_string().into()
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParserError::EndOfStream => "协议错误；意外的流结束".fmt(f),
            ParserError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParserError {}
