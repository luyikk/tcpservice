use crate::services::{ServiceHandler, ServicesCmd};
use bytes::{Buf, BufMut, Bytes};
use core::fmt;
use log::*;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::time::{delay_for, Duration};
use xbinary::{XBRead, XBWrite};

/// 客户端PEER

pub struct ClientPeer {
    pub session_id: u32,
    pub addr: SocketAddr,
    pub sender: Sender<XBWrite>,
    pub is_open_zero: AtomicBool,
    pub service_handler: ServiceHandler,
    pub last_recv_time: AtomicI64,
}

impl Display for ClientPeer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.session_id, self.addr)
    }
}

/// 用于生成 Session_Id
static MAKE_SESSION_ID: AtomicU32 = AtomicU32::new(1);

impl ClientPeer {
    /// 创建一个客户端PEER
    pub fn new(
        addr: SocketAddr,
        sender: Sender<XBWrite>,
        service_handler: ServiceHandler,
    ) -> ClientPeer {
        ClientPeer {
            session_id: Self::make_conv(),
            addr,
            sender,
            is_open_zero: AtomicBool::new(false),
            service_handler,
            last_recv_time: AtomicI64::new(Self::timestamp()),
        }
    }

    /// OPEN 服务器
    pub fn open(&self, service_id: u32) -> Result<(), SendError<ServicesCmd>> {
        self.service_handler
            .clone()
            .open(self.session_id, service_id, self.addr.to_string())?;
        info!("start open service:{} peer:{}", service_id, self.session_id);
        Ok(())
    }

    /// 服务器通知 设置OPEN成功
    pub async fn open_service(&self, service_id: u32) -> Result<(), Box<dyn Error>> {
        info!("service:{} open peer:{} OK", service_id, self.session_id);
        self.is_open_zero.store(true, Ordering::Release);
        self.send_open(service_id).await?;
        Ok(())
    }

    /// 服务器通知 关闭某个服务
    pub async fn close_service(&self, service_id: u32) -> Result<(), Box<dyn Error>> {
        info!("service:{} Close peer:{} OK", service_id, self.session_id);
        if service_id == 0 {
            self.kick().await?;
        } else {
            self.send_close(service_id).await?;
        }
        Ok(())
    }

    /// 数据包输入
    pub async fn input_buff(
        &self,
        sender: &mut Sender<XBWrite>,
        service_handler: &mut ServiceHandler,
        data: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        let mut reader = XBRead::new(data);
        let server_id = reader.get_u32_le();
        self.last_recv_time
            .store(Self::timestamp(), Ordering::Release);
        match server_id {
            //给网关发送数据包,默认当PING包无脑回
            0xFFFFFFFF => {
                self.send(sender, server_id, &reader).await?;
            }
            _ => {
                //前置检测 如果没有OPEN 0 不能发送
                if !self.is_open_zero.load(Ordering::Acquire) {
                    self.kick().await?;
                    info!("Peer:{} not open 0 read data Disconnect it", self);
                    return Ok(());
                }

                service_handler.send_buffer(self.session_id, server_id, reader)?;
            }
        }

        Ok(())
    }

    /// 先发送断线包等待多少毫秒清理内存
    pub async fn kick_wait_ms(&self, ms: i32) -> Result<(), Box<dyn Error>> {
        if ms == 3111 {
            self.disconnect_now().await?;
        } else {
            self.send_close(0).await?;
            delay_for(Duration::from_millis(ms as u64)).await;
            self.disconnect_now().await?;
        }
        Ok(())
    }

    /// 发送 CLOSE 0 后立即断线清理内存
    async fn kick(&self) -> Result<(), Box<dyn Error>> {
        self.send_close(0).await?;
        self.disconnect_now().await?;
        Ok(())
    }

    /// 立即断线,清理内存
    pub async fn disconnect_now(&self) -> Result<(), Box<dyn Error>> {
        // 先关闭OPEN 0 标志位
        self.is_open_zero.store(false, Ordering::Release);

        // 管它有没有 每个服务器都调用下 DropClientPeer 让服务器的 DropClientPeer 自己检查
        self.service_handler
            .clone()
            .disconnect_events(self.session_id)?;

        self.sender.clone().send(XBWrite::new()).await?;

        info!("peer:{} disconnect Cleanup", self);

        Ok(())
    }

    /// 发送数据
    #[inline]
    pub async fn send(
        &self,
        sender: &mut Sender<XBWrite>,
        session_id: u32,
        data: &[u8],
    ) -> Result<(), Box<dyn Error>> {
        let mut writer = XBWrite::new();
        writer.put_u32_le(0);
        writer.put_u32_le(session_id);
        writer.write(data);
        writer.set_position(0);
        writer.put_u32_le(writer.len() as u32 - 4);
        sender.send(writer).await?;
        Ok(())
    }

    /// 发送OPEN
    #[inline]
    pub async fn send_open(&self, service_id: u32) -> Result<(), Box<dyn Error>> {
        let mut writer = XBWrite::new();
        writer.put_u32_le(0);
        writer.put_u32_le(0xFFFFFFFF);
        writer.write_string_bit7_len("open");
        writer.bit7_write_u32(service_id);
        writer.set_position(0);
        writer.put_u32_le(writer.len() as u32 - 4);
        self.sender.clone().send(writer).await?;
        Ok(())
    }

    /// 发送CLOSE 0
    #[inline]
    pub async fn send_close(&self, service_id: u32) -> Result<(), Box<dyn Error>> {
        let mut writer = XBWrite::new();
        writer.put_u32_le(0);
        writer.put_u32_le(0xFFFFFFFF);
        writer.write_string_bit7_len("close");
        writer.bit7_write_u32(service_id);
        writer.set_position(0);
        writer.put_u32_le(writer.len() as u32 - 4);
        self.sender.clone().send(writer).await?;
        Ok(())
    }

    /// 获取时间戳
    #[inline]
    fn timestamp() -> i64 {
        chrono::Local::now().timestamp_nanos() / 100
    }

    /// 生成一个u32的conv
    #[inline]
    fn make_conv() -> u32 {
        let old = MAKE_SESSION_ID.fetch_add(1, Ordering::Release);
        if old == u32::max_value() - 1 {
            MAKE_SESSION_ID.store(1, Ordering::Release);
        }
        old
    }
}
