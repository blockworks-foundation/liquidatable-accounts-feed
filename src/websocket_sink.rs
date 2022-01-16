use {
    crate::Config,
    anyhow::Context,
    futures_util::{SinkExt, StreamExt},
    log::*,
    tokio::net::{TcpListener, TcpStream},
    //serde_derive::Deserialize,
    //solana_sdk::pubkey::Pubkey,
    //std::str::FromStr,
    tokio::sync::broadcast,
};

#[derive(Clone, Debug)]
pub enum LiquidatableInfo {
    Start,
    Stop,
}

async fn accept_connection(
    stream: TcpStream,
    mut rx: broadcast::Receiver<LiquidatableInfo>,
) -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::Message;

    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!("new tcp client at address: {}", addr);

    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("error during the websocket handshake");
    info!("new websocket client at address: {}", addr);

    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1000));

    loop {
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => ws_stream.send(Message::Pong(data)).await?,
                    Some(Ok(_)) => continue, // ignore other incoming
                    None | Some(Err(_)) => break, // disconnected
                }
            },
            msg = rx.recv() => {
                if msg.is_err() {
                    // broadcast stream is lagging or disconnected
                    // -> drop websocket connection
                    warn!("liquidation info broadcast receiver had error: {:?}", msg);
                    ws_stream.close(None).await?;
                    break;
                }
                let msg = msg.unwrap();

                ws_stream.send(Message::Text("msg".to_owned())).await?;
            },
            _ = interval.tick() => {
                ws_stream.send(Message::Ping(vec![])).await?;
            },
        }
    }

    Ok(())
}

pub async fn start(config: Config) -> anyhow::Result<broadcast::Sender<LiquidatableInfo>> {
    // The channel that liquidatable event changes are sent through, to
    // be forwarded to websocket clients
    let (tx, _) = broadcast::channel(1000);

    let websocket_listener = TcpListener::bind(&config.websocket_server_bind_address)
        .await
        .context("binding websocket server")?;
    info!(
        "websocket server listening on: {}",
        &config.websocket_server_bind_address
    );
    let tx_c = tx.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = websocket_listener.accept().await {
            tokio::spawn(accept_connection(stream, tx_c.subscribe()));
        }
    });

    Ok(tx)
}
