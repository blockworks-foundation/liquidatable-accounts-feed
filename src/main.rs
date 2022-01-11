pub mod metrics;
pub mod snapshot_source;
pub mod websocket_source;

use {
    anyhow::Context,
    async_trait::async_trait,
    fixed::types::I80F48,
    log::*,
    mango::state::{
        DataType, HealthCache, HealthType, MangoAccount, MangoCache, MangoGroup, UserActiveAssets,
        MAX_PAIRS,
    },
    mango_common::Loadable,
    serde_derive::Deserialize,
    solana_sdk::account::{AccountSharedData, ReadableAccount},
    solana_sdk::account_info::AccountInfo,
    solana_sdk::pubkey::Pubkey,
    std::collections::{HashMap, HashSet},
    std::fs::File,
    std::io::Read,
    std::str::FromStr,
    std::sync::Arc,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SlotStatus {
    Rooted,
    Confirmed,
    Processed,
}

#[derive(Clone, Debug)]
pub struct SlotData {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: SlotStatus,
    pub chain: u64, // the top slot that this is in a chain with. uncles will have values < tip
}

#[derive(Clone, Debug)]
pub struct AccountData {
    pub slot: u64,
    pub account: AccountSharedData,
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
}

pub fn encode_address(addr: &Pubkey) -> String {
    bs58::encode(&addr.to_bytes()).into_string()
}

#[derive(Default)]
struct ChainData {
    slots: HashMap<u64, SlotData>,
    accounts: HashMap<Pubkey, Vec<AccountData>>,
    newest_rooted_slot: u64,
    newest_processed_slot: u64,
}

impl ChainData {
    fn update_slot(&mut self, new_slot: SlotData) {
        let new_processed_head = new_slot.slot > self.newest_processed_slot;
        if new_processed_head {
            self.newest_processed_slot = new_slot.slot;
        }

        let new_rooted_head =
            new_slot.slot > self.newest_rooted_slot && new_slot.status == SlotStatus::Rooted;
        if new_rooted_head {
            self.newest_rooted_slot = new_slot.slot;
        }

        let mut parent_update = false;

        use std::collections::hash_map::Entry;
        match self.slots.entry(new_slot.slot) {
            Entry::Vacant(v) => {
                v.insert(new_slot);
            }
            Entry::Occupied(o) => {
                let v = o.into_mut();
                parent_update = v.parent != new_slot.parent && new_slot.parent.is_some();
                v.parent = v.parent.or(new_slot.parent);
                v.status = new_slot.status;
            }
        };

        if new_processed_head || parent_update {
            // update the "chain" field down to the first rooted slot
            let mut slot = self.newest_processed_slot;
            loop {
                if let Some(data) = self.slots.get_mut(&slot) {
                    data.chain = self.newest_processed_slot;
                    if data.status == SlotStatus::Rooted {
                        break;
                    }
                    if let Some(parent) = data.parent {
                        slot = parent;
                        continue;
                    }
                }
                break;
            }
        }

        if new_rooted_head {
            // for each account, preserve only writes > newest_rooted_slot, or the newest
            // rooted write
            for (_, writes) in self.accounts.iter_mut() {
                let newest_rooted_write = writes
                    .iter()
                    .rev()
                    .find(|w| {
                        w.slot <= self.newest_rooted_slot
                            && self
                                .slots
                                .get(&w.slot)
                                .map(|s| {
                                    // sometimes we seem not to get notifications about slots
                                    // getting rooted, hence assume non-uncle slots < newest_rooted_slot
                                    // are rooted too
                                    s.status == SlotStatus::Rooted
                                        || s.chain == self.newest_processed_slot
                                })
                                // preserved account writes for deleted slots <= newest_rooted_slot
                                // are expected to be rooted
                                .unwrap_or(true)
                    })
                    .map(|w| w.slot)
                    // no rooted write found: produce no effect, since writes > newest_rooted_slot are retained anyway
                    .unwrap_or(self.newest_rooted_slot + 1);
                writes
                    .retain(|w| w.slot == newest_rooted_write || w.slot > self.newest_rooted_slot);
            }

            // now it's fine to drop any slots before the new rooted head
            // as account writes for non-rooted slots before it have been dropped
            self.slots.retain(|s, _| *s >= self.newest_rooted_slot);
        }
    }

    fn update_account(&mut self, pubkey: Pubkey, account: AccountData) {
        use std::collections::hash_map::Entry;
        match self.accounts.entry(pubkey) {
            Entry::Vacant(v) => {
                v.insert(vec![account]);
            }
            Entry::Occupied(o) => {
                let v = o.into_mut();
                // v is ordered by slot ascending. find the right position
                // overwrite if an entry for the slot already exists, otherwise insert
                let rev_pos = v
                    .iter()
                    .rev()
                    .position(|d| d.slot <= account.slot)
                    .unwrap_or(v.len());
                let pos = v.len() - rev_pos;
                if pos < v.len() && v[pos].slot == account.slot {
                    v[pos] = account;
                } else {
                    v.insert(pos, account);
                }
            }
        };
    }

    fn update_from_snapshot(&mut self, snapshot: snapshot_source::AccountSnapshot) {
        for account_write in snapshot.accounts {
            self.update_account(
                account_write.pubkey,
                AccountData {
                    slot: account_write.slot,
                    account: account_write.account,
                },
            );
        }
    }

    fn update_from_websocket(&mut self, message: websocket_source::Message) {
        match message {
            websocket_source::Message::Account(account_write) => {
                trace!("websocket account message");
                self.update_account(
                    account_write.pubkey,
                    AccountData {
                        slot: account_write.slot,
                        account: account_write.account,
                    },
                );
            }
            websocket_source::Message::Slot(slot_update) => {
                trace!("websocket slot message");
                let slot_update = match *slot_update {
                    solana_client::rpc_response::SlotUpdate::CreatedBank {
                        slot, parent, ..
                    } => Some(SlotData {
                        slot,
                        parent: Some(parent),
                        status: SlotStatus::Processed,
                        chain: 0,
                    }),
                    solana_client::rpc_response::SlotUpdate::OptimisticConfirmation {
                        slot,
                        ..
                    } => Some(SlotData {
                        slot,
                        parent: None,
                        status: SlotStatus::Confirmed,
                        chain: 0,
                    }),
                    solana_client::rpc_response::SlotUpdate::Root { slot, .. } => Some(SlotData {
                        slot,
                        parent: None,
                        status: SlotStatus::Rooted,
                        chain: 0,
                    }),
                    _ => None,
                };
                if let Some(update) = slot_update {
                    self.update_slot(update);
                }
            }
        }
    }

    fn is_account_write_live(&self, write: &AccountData) -> bool {
        self.slots
            .get(&write.slot)
            // either the slot is rooted or in the current chain
            .map(|s| s.status == SlotStatus::Rooted || s.chain == self.newest_processed_slot)
            // if the slot can't be found but preceeds newest rooted, use it too (old rooted slots are removed)
            .unwrap_or(write.slot <= self.newest_rooted_slot)
    }

    /// Cloned snapshot of all the most recent live writes per pubkey
    fn accounts_snapshot(&self) -> HashMap<Pubkey, AccountData> {
        self.accounts
            .iter()
            .filter_map(|(pubkey, writes)| {
                let latest_good_write = writes
                    .iter()
                    .rev()
                    .find(|w| self.is_account_write_live(w))?;
                Some((pubkey.clone(), latest_good_write.clone()))
            })
            .collect()
    }

    /// Ref to the most recent live write of the pubkey
    fn account<'a>(&'a self, pubkey: &Pubkey) -> anyhow::Result<&'a AccountSharedData> {
        self.accounts
            .get(pubkey)
            .ok_or(anyhow::anyhow!("account {} not found", pubkey))?
            .iter()
            .rev()
            .find(|w| self.is_account_write_live(w))
            .ok_or(anyhow::anyhow!("account {} has no live data", pubkey))
            .map(|w| &w.account)
    }
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

fn check_health_single(
    chain_data: &ChainData,
    group_id: &Pubkey,
    cache_id: &Pubkey,
    account_id: &Pubkey,
) -> anyhow::Result<I80F48> {
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
    let health = health_cache.get_health(group, HealthType::Maint);

    Ok(health)
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

    let metrics_tx = metrics::start();

    let (websocket_sender, websocket_receiver) =
        async_channel::unbounded::<websocket_source::Message>();
    websocket_source::start(config.clone(), websocket_sender);

    // Wait for some websocket data to accumulate before requesting snapshots,
    // to make it more likely that
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    let (snapshot_sender, snapshot_receiver) =
        async_channel::unbounded::<snapshot_source::AccountSnapshot>();
    snapshot_source::start(config.clone(), snapshot_sender);

    let mut chain_data = ChainData::default();
    let mut mango_accounts = HashSet::<Pubkey>::new();

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

                                // TODO: check health of this particular one
                                if let Err(err) = check_health_single(
                                        &chain_data,
                                        &mango_group_id,
                                        &mango_cache_id,
                                        &account_write.pubkey
                                    ) {
                                    warn!("error computing health of {}: {:?}", account_write.pubkey, err);
                                }
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
                                    let health = check_health_single(&chain_data, &mango_group_id, &mango_cache_id, &pubkey);
                                    match health {
                                        Ok(value) => {
                                            if value < 0 { info!("account {} has negative health {}", pubkey, value) }
                                        },
                                        Err(err) => {
                                            warn!("error computing health of {}: {:?}", pubkey, err);
                                        },

                                    }
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
