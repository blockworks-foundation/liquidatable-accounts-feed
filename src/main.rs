pub mod chain_data;
pub mod metrics;
pub mod snapshot_source;
pub mod websocket_sink;
pub mod websocket_source;

use {
    crate::chain_data::*,
    crate::websocket_sink::LiquidatableInfo,
    anyhow::Context,
    fixed::types::I80F48,
    log::*,
    mango::state::{
        DataType, HealthCache, HealthType, MangoAccount, MangoCache, MangoGroup, UserActiveAssets,
        MAX_PAIRS,
    },
    mango_common::Loadable,
    serde_derive::Deserialize,
    solana_sdk::account::{AccountSharedData, ReadableAccount},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashSet,
    std::fs::File,
    std::io::Read,
    std::str::FromStr,
    tokio::sync::broadcast,
};

trait AnyhowWrap {
    type Value;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
    type Value = T;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
        self.map_err(|err| anyhow::anyhow!("{:?}", err))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub rpc_ws_url: String,
    pub rpc_http_url: String,
    pub mango_program_id: String,
    pub mango_group_id: String,
    pub mango_cache_id: String,
    pub mango_signer_id: String,
    pub serum_program_id: String,
    pub snapshot_interval_secs: u64,
    pub websocket_server_bind_address: String,
}

pub fn encode_address(addr: &Pubkey) -> String {
    bs58::encode(&addr.to_bytes()).into_string()
}

// FUTURE: It'd be very nice if I could map T to the DataType::T constant!
fn load_mango_account<T: Loadable + Sized>(
    data_type: DataType,
    account: &AccountSharedData,
) -> anyhow::Result<&T> {
    let data = account.data();
    let data_type_int = data_type as u8;
    if data.len() != std::mem::size_of::<T>() {
        anyhow::bail!(
            "bad account size for {}: {} expected {}",
            data_type_int,
            data.len(),
            std::mem::size_of::<T>()
        );
    }
    if data[0] != data_type_int {
        anyhow::bail!(
            "unexpected data type for {}, got {}",
            data_type_int,
            data[0]
        );
    }
    return Ok(Loadable::load_from_bytes(&data).expect("always Ok"));
}

pub fn load_open_orders(
    account: &AccountSharedData,
) -> anyhow::Result<&serum_dex::state::OpenOrders> {
    let data = account.data();
    let expected_size = 12 + std::mem::size_of::<serum_dex::state::OpenOrders>();
    if data.len() != expected_size {
        anyhow::bail!(
            "bad open orders account size: {} expected {}",
            data.len(),
            expected_size
        );
    }
    if &data[0..5] != "serum".as_bytes() {
        anyhow::bail!("unexpected open orders account prefix");
    }
    Ok(bytemuck::from_bytes::<serum_dex::state::OpenOrders>(
        &data[5..data.len() - 7],
    ))
}

fn get_open_orders<'a>(
    chain_data: &'a ChainData,
    group: &MangoGroup,
    account: &'a MangoAccount,
) -> anyhow::Result<Vec<Option<&'a serum_dex::state::OpenOrders>>> {
    let mut unpacked = vec![None; MAX_PAIRS];
    for i in 0..group.num_oracles {
        if account.in_margin_basket[i] {
            let oo = chain_data.account(&account.spot_open_orders[i])?;
            unpacked[i] = Some(load_open_orders(oo)?);
        }
    }
    Ok(unpacked)
}

#[derive(Debug)]
struct IsLiquidatable {
    liquidatable: bool,
    being_liquidated: bool,
    health: I80F48, // can be init or maint, depending on being_liquidated
}

fn compute_liquidatable_single(
    chain_data: &ChainData,
    group_id: &Pubkey,
    cache_id: &Pubkey,
    account_id: &Pubkey,
) -> anyhow::Result<IsLiquidatable> {
    let group = load_mango_account::<MangoGroup>(
        DataType::MangoGroup,
        chain_data
            .account(group_id)
            .context("getting group account")?,
    )?;
    let cache = load_mango_account::<MangoCache>(
        DataType::MangoCache,
        chain_data
            .account(cache_id)
            .context("getting cache account")?,
    )?;
    let account = load_mango_account::<MangoAccount>(
        DataType::MangoAccount,
        chain_data
            .account(account_id)
            .context("getting user account")?,
    )?;
    let oos = get_open_orders(chain_data, group, account).context("getting user open orders")?;

    let assets = UserActiveAssets::new(group, account, vec![]);
    let mut health_cache = HealthCache::new(assets);
    health_cache.init_vals_with_orders_vec(group, cache, account, &oos)?;

    let health_type = if account.being_liquidated {
        HealthType::Init
    } else {
        HealthType::Maint
    };
    let health = health_cache.get_health(group, health_type);

    Ok(IsLiquidatable {
        liquidatable: health < 0,
        being_liquidated: account.being_liquidated,
        health,
    })
}

fn process_account(
    chain_data: &ChainData,
    group_id: &Pubkey,
    cache_id: &Pubkey,
    account_id: &Pubkey,
    currently_liquidatable: &mut HashSet<Pubkey>,
    tx: &broadcast::Sender<LiquidatableInfo>,
) {
    let res = compute_liquidatable_single(chain_data, group_id, cache_id, account_id);
    if let Err(err) = res {
        warn!("error computing health of {}: {:?}", account_id, err);
        return;
    }
    let res = res.unwrap();
    let was_liquidatable = currently_liquidatable.contains(account_id);
    if res.liquidatable && !was_liquidatable {
        info!("account {} is newly liquidatable: {:?}", account_id, res);
        currently_liquidatable.insert(account_id.clone());
        let _ = tx.send(LiquidatableInfo::Start {
            account: account_id.clone(),
        });
    }
    if !res.liquidatable && was_liquidatable {
        info!("account {} stopped being liquidatable", account_id);
        currently_liquidatable.remove(account_id);
        let _ = tx.send(LiquidatableInfo::Stop {
            account: account_id.clone(),
        });
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("requires a config file argument");
        return Ok(());
    }

    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    let mango_program_id = Pubkey::from_str(&config.mango_program_id)?;
    let mango_group_id = Pubkey::from_str(&config.mango_group_id)?;
    let mango_cache_id = Pubkey::from_str(&config.mango_cache_id)?;

    solana_logger::setup_with_default("info");
    info!("startup");

    let metrics = metrics::start();

    // Information about liquidatable accounts is sent through this channel
    // and then forwarded to all connected websocket clients
    let liquidatable_sender = websocket_sink::start(config.clone()).await?;

    // Sourcing account and slot data from solana via websockets
    let (websocket_sender, websocket_receiver) =
        async_channel::unbounded::<websocket_source::Message>();
    websocket_source::start(config.clone(), websocket_sender);

    // Wait for some websocket data to accumulate before requesting snapshots,
    // to make it more likely that
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    // Getting solana account snapshots via jsonrpc
    let (snapshot_sender, snapshot_receiver) =
        async_channel::unbounded::<snapshot_source::AccountSnapshot>();
    snapshot_source::start(config.clone(), snapshot_sender);

    let mut chain_data = ChainData::new(&metrics);
    let mut mango_accounts = HashSet::<Pubkey>::new();
    let mut currently_liquidatable = HashSet::<Pubkey>::new();

    let mut one_snapshot_done = false;

    info!("main loop");
    loop {
        tokio::select! {
            message = websocket_receiver.recv() => {
                let message = message.expect("channel not closed");

                // build a model of slots and accounts in `chain_data`
                // this code should be generic so it can be reused in future projects
                chain_data.update_from_websocket(message.clone());

                // specific program logic using the mirrored data
                match message {
                    websocket_source::Message::Account(account_write) => {
                        let data = account_write.account.data();

                        // TODO: Do we need to check health when open orders accounts change?
                        if account_write.account.owner() != &mango_program_id || data.len() == 0 {
                            continue;
                        }

                        let kind = DataType::try_from(data[0]).unwrap();
                        match kind {
                            // MangoAccount
                            DataType::MangoAccount => {
                                if data.len() != std::mem::size_of::<MangoAccount>() {
                                    continue;
                                }
                                let data = MangoAccount::load_from_bytes(&data).expect("always Ok");
                                if data.mango_group != mango_group_id {
                                    continue;
                                }

                                // Track all MangoAccounts: we need to iterate over them later
                                mango_accounts.insert(account_write.pubkey);

                                if !one_snapshot_done {
                                    continue;
                                }

                                process_account(
                                        &chain_data,
                                        &mango_group_id,
                                        &mango_cache_id,
                                        &account_write.pubkey,
                                        &mut currently_liquidatable,
                                        &liquidatable_sender,
                                    );
                            }
                            // MangoCache
                            DataType::MangoCache => {
                                if account_write.pubkey != mango_cache_id {
                                    continue;
                                }

                                if !one_snapshot_done {
                                    continue;
                                }

                                // check health of all accounts

                                // TODO: This could be done asynchronously by calling
                                // let accounts = chain_data.accounts_snapshot();
                                // and then working with the snapshot of the data

                                for pubkey in mango_accounts.iter() {
                                    // TODO: This is slowed down by fetching env for each key
                                    process_account(
                                            &chain_data,
                                            &mango_group_id,
                                            &mango_cache_id,
                                            &pubkey,
                                            &mut currently_liquidatable,
                                            &liquidatable_sender,
                                        );
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            },
            message = snapshot_receiver.recv() => {
                let message = message.expect("channel not closed");
                chain_data.update_from_snapshot(message);
                one_snapshot_done = true;

                // TODO: trigger a full health check
            },
        }
    }
}
