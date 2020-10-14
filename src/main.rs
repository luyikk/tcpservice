mod tcp;

use tcp::TCPServer;
use std::error::Error;
use log::*;
use env_logger::Builder;
use crate::tcp::TCPPeer;
use tokio::io::AsyncReadExt;
use std::result::Result::Ok;
use std::io::Write;
use xbinary::XBWrite;
use bytes::BufMut;

#[tokio::main]
async fn main()->Result<(),Box<dyn Error>> {
    Builder::new().filter_level(LevelFilter::Debug).init();
    let tcpserver= TCPServer::<_,_>::new("127.0.0.1:8080", buff_input).await?;
    tcpserver.set_connection_event(|addr|{
        info!("ipaddress:{} connect",addr);
        return true;
    });
    tcpserver.start().await?;
    Ok(())
}

async fn buff_input(mut peer:TCPPeer){
    while let Ok(char)= peer.reader.read_u8().await {

        print!("{}",char as char);
        std::io::stdout().flush();

        let mut writer =XBWrite::new();
        writer.put_u8(char);
        peer.sender.send(writer).await.unwrap();
        
    }

    println!("{} disconnect",peer.addr);
}