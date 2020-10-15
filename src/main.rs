#![allow(dead_code)]
mod tcp;
mod users;
mod services;

use crate::tcp::TCPPeer;
use crate::users::ClientPeer;
use users::UserClientManager;
use services::ServicesManager;
use env_logger::Builder;
use log::*;
use std::error::Error;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tcp::TCPServer;
use tokio::io::AsyncReadExt;
use mimalloc::MiMalloc;
use lazy_static::lazy_static;
use bytes::Bytes;
use json::JsonValue;

/// 最大数据表长度限制 4M
const MAX_BUFF_LEN:usize=4*1024*1024;


#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

lazy_static! {

    /// 配置文件
    pub static ref SERVICE_CFG:JsonValue={
        if let Ok(json)= std::fs::read_to_string("./service_cfg.json") {
            json::parse(&json).unwrap()
        }
        else{
            panic!("not found service_cfg.json");
        }
    };

    /// 用户管理
    pub static ref USER_PEER_MANAGER: Arc<UserClientManager> = UserClientManager::new();

    /// 服务管理
    pub static ref SERVICE_MANAGER:Arc<ServicesManager>=ServicesManager::new(&SERVICE_CFG,USER_PEER_MANAGER.get_handle()).unwrap();
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    Builder::new().filter_level(LevelFilter::Debug).init();
    SERVICE_MANAGER.start().await?;
    USER_PEER_MANAGER.set_service_handler(SERVICE_MANAGER.get_handler());

    let tcpserver = TCPServer::<_, _>::new(format!("0.0.0.0:{}",SERVICE_CFG["listenPort"].as_i32().unwrap()), buff_input).await?;
    tcpserver.set_connection_event(|addr| {
        info!("addr:{} connect", addr);
        true
    });
    tcpserver.start().await?;
    Ok(())
}

/// 数据包输入
async fn buff_input(mut peer: TCPPeer) {
    //创建一个客户端PEER,同时把地址和发送克隆进去
    let client_peer = Arc::new(ClientPeer::new(peer.addr, peer.get_sender(),SERVICE_MANAGER.get_handler()));
    //通知客户端管理器创建一个客户端
    let mut manager_handler=USER_PEER_MANAGER.get_handle();
    if let Err(er)=manager_handler.create_peer(client_peer.clone()){
        error!("create peer:{} error:{}->{:?}",client_peer,er,er);
        return;
    }
    let mut sender =peer.get_sender();
    let mut service_handler=SERVICE_MANAGER.get_handler();
    debug!("create peer:{}", client_peer.session_id);
    match client_peer.open(0) {
        Ok(_) => {
            //读取数据包长度
            while let Ok(packer_len) = peer.reader.read_u32_le().await {
                let packer_len:usize=packer_len as usize;
                //如果没有OPEN 直接掐线
                if !client_peer.is_open_zero.load(Ordering::Acquire) {
                    warn!("peer:{} not open send data,disconnect!", client_peer);
                    break;
                }
                // 如果长度为0 或者超过最大限制 掐线
                if packer_len == 0 || packer_len >MAX_BUFF_LEN{
                    warn!("disconnect peer:{} packer len error:{}",client_peer,packer_len);
                    break;
                }

                // 创建一个vector 用来接收指定长度的数据,这样就可以避免粘包问题
                let mut data = vec![0; packer_len];
                match peer.reader.read_exact(&mut data).await {
                    Ok(len) if len == 0 => {
                        warn!("disconnect peer:{} len is 0 ",client_peer);
                        break;
                    }
                    Err(er) => {
                        error!("peer:{} read data error:{}->{:?}", client_peer, er, er);
                        break;
                    }
                    Ok(len) => {
                        if len == packer_len {
                            //数据包读取成功 输入逻辑处理
                            if let Err(er) = client_peer.input_buff(&mut sender, &mut service_handler, Bytes::from(data)).await {
                                error!("peer:{} input buff error:{}->{:?}", client_peer, er, er);
                                break;
                            }
                        }else{
                            warn!("disconnect peer:{} read len error:{}!={}",client_peer,len,packer_len);
                            break;
                        }
                    }
                }
            }
        }
        Err(er) => error!("peer:{} open err:{}->{:?}", client_peer, er, er),
    }
    //断线处理删除用户管理器里的PEER 彻底删除PEER
    if let Err(er)= manager_handler.remove_peer(client_peer.session_id){
        error!("remove peer:{} error:{}->{:?}",client_peer,er,er);
    }
    info!("{} disconnect", client_peer);
}
