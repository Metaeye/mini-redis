//! 提供一个表示 Redis 协议帧的类型以及从字节数组解析帧的实用程序。

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// Redis 协议中的帧。
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum FrameError {
    /// 没有足够的数据来解析消息
    Incomplete,
    /// 无效的消息编码
    Other(crate::Error),
}

impl Frame {
    /// 返回一个空数组
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// 将一个“bulk”帧推入数组。`self` 必须是一个 Array 帧。
    ///
    /// # Panics
    ///
    /// 如果 `self` 不是数组，则会 panic
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// 将一个“integer”帧推入数组。`self` 必须是一个 Array 帧。
    ///
    /// # Panics
    ///
    /// 如果 `self` 不是数组，则会 panic
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// 检查是否可以从 `src` 解码整个消息
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), FrameError> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // 跳过 '-1\r\n'
                    skip(src, 4)
                } else {
                    // 读取 bulk 字符串
                    let len: usize = get_decimal(src)?.try_into()?;

                    // 跳过该数量的字节 + 2 (\r\n)。
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(src)?;

                (0..len).try_for_each(|_| Frame::check(src))
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }
}

impl From<&mut Cursor<&[u8]>> for Frame {
    /// 消息已经通过 `check` 验证。
    fn from(src: &mut Cursor<&[u8]>) -> Frame {
        match get_u8(src).unwrap() {
            b'+' => {
                // 读取行并将其转换为 `Vec<u8>`
                let line = get_line(src).unwrap().to_vec();
                // 将行转换为 String
                let string = String::from_utf8(line).unwrap();

                Frame::Simple(string)
            }
            b'-' => {
                // 读取行并将其转换为 `Vec<u8>`
                let line = get_line(src).unwrap().to_vec();
                // 将行转换为 String
                let string = String::from_utf8(line).unwrap();

                Frame::Error(string)
            }
            b':' => {
                let len = get_decimal(src).unwrap();

                Frame::Integer(len)
            }
            b'$' => {
                if b'-' == peek_u8(src).unwrap() {
                    let _ = get_line(src);

                    Frame::Null
                } else {
                    // 读取 bulk 字符串
                    let len = get_decimal(src).unwrap().try_into().unwrap();
                    let bytes = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // 跳过该数量的字节 + 2 (\r\n)。
                    skip(src, len + 2).unwrap();

                    Frame::Bulk(bytes)
                }
            }
            b'*' => {
                let len = get_decimal(src).unwrap().try_into().unwrap();
                // 必须顺序执行map, 不可以使用par_iter, 否则会导致顺序错乱
                let vec = (0..len).map(|_| Frame::from(&mut *src)).collect();

                Frame::Array(vec)
            }
            _ => unimplemented!(),
        }
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                parts.iter().enumerate().try_for_each(|(i, part)| {
                    if i > 0 {
                        // 使用空格作为数组元素显示分隔符
                        write!(fmt, " ")?;
                    }

                    part.fmt(fmt)
                })
            }
        }
    }
}

impl From<&Frame> for crate::Error {
    /// 将帧转换为“unexpected frame”错误
    fn from(frame: &Frame) -> Self {
        format!("unexpected frame: {}", frame).into()
    }
}

impl From<String> for FrameError {
    fn from(src: String) -> FrameError {
        FrameError::Other(src.into())
    }
}

impl From<&str> for FrameError {
    fn from(src: &str) -> FrameError {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for FrameError {
    fn from(_src: FromUtf8Error) -> FrameError {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for FrameError {
    fn from(_src: TryFromIntError) -> FrameError {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for FrameError {}

impl fmt::Display for FrameError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FrameError::Incomplete => "stream ended early".fmt(fmt),
            FrameError::Other(err) => err.fmt(fmt),
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }

    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }

    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), FrameError> {
    if src.remaining() < n {
        return Err(FrameError::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// 读取一个以新行终止的十进制数
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, FrameError> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// 查找一行
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], FrameError> {
    // 直接扫描字节
    let start = src.position() as usize;
    // 扫描到倒数第二个字节
    let end = src.get_ref().len() - 1;

    (start..end)
        .find(|&i| src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n')
        .map(|i| {
            // 找到一行，更新位置到 \n 之后
            src.set_position((i + 2) as u64);
            // 返回该行
            &src.get_ref()[start..i]
        })
        .ok_or(FrameError::Incomplete)
}
