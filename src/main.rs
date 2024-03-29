#![allow(dead_code)]
#![feature(async_closure)]
mod services;
mod stdout_log;
mod tcp;
mod users;

use crate::stdout_log::StdErrLog;
use crate::tcp::TCPPeer;
use crate::users::ClientPeer;
use bytes::{Bytes};
use flexi_logger::{Age, Cleanup, Criterion, LogTarget, Naming};
use json::JsonValue;
use lazy_static::lazy_static;
use mimalloc::MiMalloc;
use services::ServicesManager;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tcp::TCPServer;
use tokio::io::AsyncReadExt;
use users::UserClientManager;
use log::*;
use anyhow::Result;
use structopt::*;


/// 最大数据表长度限制 512K
const MAX_BUFF_LEN: usize = 512 * 1024;

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
    pub static ref USER_PEER_MANAGER: Arc<UserClientManager> = UserClientManager::new(SERVICE_CFG["clientTimeoutSeconds"].as_u32().unwrap());

    /// 服务管理
    pub static ref SERVICE_MANAGER:Arc<ServicesManager>=ServicesManager::new(&SERVICE_CFG,USER_PEER_MANAGER.get_handle()).unwrap();
}

#[tokio::main]
async fn main() -> Result<()> {

    //Builder::new().filter_level(LevelFilter::Debug).init();
    init_log_system();
    SERVICE_MANAGER.start().await?;
    USER_PEER_MANAGER.set_service_handler(SERVICE_MANAGER.get_handler());

    let tcpserver = TCPServer::<_, _>::new(
        format!("0.0.0.0:{}", SERVICE_CFG["listenPort"].as_i32().unwrap()),
        buff_input,
    )
        .await?;
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
    let client_peer = Arc::new(ClientPeer::new(
        peer.addr,
        peer.get_sender(),
        SERVICE_MANAGER.get_handler(),
    ));
    //通知客户端管理器创建一个客户端
    let mut manager_handler = USER_PEER_MANAGER.get_handle();
    if let Err(er) = manager_handler.create_peer(client_peer.clone()) {
        error!("create peer:{} error:{}->{:?}", client_peer, er, er);
        return;
    }
    let mut sender = peer.get_sender();
    let mut service_handler = SERVICE_MANAGER.get_handler();
    debug!("create peer:{}", client_peer.session_id);
    match client_peer.open(0) {
        Ok(_) => {
            //读取数据包长度
            while let Ok(packer_len) = peer.reader.read_u32_le().await {
                let packer_len: usize = packer_len as usize;
                //如果没有OPEN 直接掐线
                if !client_peer.is_open_zero.load(Ordering::Acquire) {
                    warn!("peer:{} not open send data,disconnect!", client_peer);
                    break;
                }
                // 如果长度为0 或者超过最大限制 掐线
                if packer_len >= MAX_BUFF_LEN || packer_len<=4{
                    warn!(
                        "disconnect peer:{} packer len error:{}",
                        client_peer, packer_len
                    );
                    break;
                }

                let mut data=vec![0;packer_len];
                match peer.reader.read_exact(&mut data).await {
                    Err(er) => {
                        error!("peer:{} read data error:{}->{:?}", client_peer, er, er);
                        break;
                    }
                    Ok(len) => {
                        if len == packer_len {
                            //数据包读取成功 输入逻辑处理
                            if let Err(er) = client_peer
                                .input_buff(&mut sender, &mut service_handler, Bytes::from(data))
                                .await
                            {
                                error!("peer:{} input buff error:{}->{:?}", client_peer, er, er);
                                break;
                            }
                        }
                        else{
                            error!("peer:{} input buff len error len:{}!={}", client_peer,len ,packer_len);
                            break;
                        }
                    }
                }
            }
        }
        Err(er) => error!("peer:{} open err:{}->{:?}", client_peer, er, er),
    }
    //断线处理删除用户管理器里的PEER 彻底删除PEER
    if let Err(er) = manager_handler.remove_peer(client_peer.session_id) {
        error!("remove peer:{} error:{}->{:?}", client_peer, er, er);
    }
    info!("{} disconnect", client_peer);

}

#[derive(StructOpt, Debug)]
#[structopt(name = "tcp gateway server")]
#[structopt(version=version())]
struct Opt{
    /// 是否显示 日志 到控制台
    #[structopt(short, long)]
    stdlog:bool,
    /// 是否打印崩溃堆栈
    #[structopt(short, long)]
    backtrace:bool
}


/// 安装日及系统
fn init_log_system() {

    let opt=Opt::from_args();
    if opt.backtrace{
        std::env::set_var("RUST_BACKTRACE","1");
    }

    let mut show_std =opt.stdlog;
    for (name, arg) in std::env::vars() {
        if name.trim() == "STDLOG" && arg.trim() == "1" {
            show_std = true;
            println!("open stderr log out");
        }
    }

    let mut log_set = LogTarget::File;
    if show_std {
        log_set = LogTarget::FileAndWriter(Box::new(StdErrLog::new()));
    }

    flexi_logger::Logger::with_str("debug")
        .log_target(log_set)
        .suffix("log")
        .directory("logs")
        .rotate(
            Criterion::AgeOrSize(Age::Day, 1024 * 1024 * 5),
            Naming::Numbers,
            Cleanup::KeepLogFiles(30),
        )
        .print_message()
        .format(flexi_logger::opt_format)
        .set_palette("196;190;6;7;8".into())
        .start()
        .unwrap();
}


#[inline(always)]
fn version()->&'static str{
    concat! {
    "\n",
    "==================================version info==================================",
    "\n",
    "Build Timestamp:", env!("VERGEN_BUILD_TIMESTAMP"), "\n",
    "GIT BRANCH:", env!("VERGEN_GIT_BRANCH"), "\n",
    "GIT COMMIT DATE:", env!("VERGEN_GIT_COMMIT_TIMESTAMP"), "\n",
    "GIT SHA:", env!("VERGEN_GIT_SHA"), "\n",
    "PROFILE:", env!("VERGEN_CARGO_PROFILE"), "\n",
    "==================================version end==================================",
    "\n",
    }
}