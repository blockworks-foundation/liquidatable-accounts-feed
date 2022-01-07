pub mod metrics;
pub mod websocket_source;

use {
    async_trait::async_trait,
    log::*,
    serde_derive::Deserialize,
    solana_sdk::{account::Account, pubkey::Pubkey},
    std::collections::BTreeMap,
    std::collections::HashMap,
    std::fs::File,
    std::io::Read,
    std::sync::Arc,
    std::str::FromStr,
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
    pub account: Account,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub rpc_ws_url: String,
    pub rpc_http_url: String,
    pub mango_program_id: String,
    pub mango_group_id: String,
    pub serum_program_id: String,
    pub mango_signer_id: String,
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

    fn update_from_websocket(&mut self, message: websocket_source::Message) {
        match message {
            websocket_source::Message::Account(account_write) => {
                info!("single message");
                self.update_account(
                    account_write.pubkey,
                    AccountData {
                        slot: account_write.slot,
                        account: account_write.account,
                    },
                );
            }
            websocket_source::Message::Slot(slot_update) => {
                info!("slot message");
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

    solana_logger::setup_with_default("info");
    info!("startup");

    let metrics_tx = metrics::start();

    let (websocket_sender, websocket_receiver) =
        async_channel::unbounded::<websocket_source::Message>();
    websocket_source::start(config, websocket_sender);

    // TODO: Also have a snapshot source

    let mut chain_data = ChainData::default();

    loop {
        let message = websocket_receiver.recv().await.unwrap();
        info!("got message");

        // build a model of slots and accounts in `chain_data`
        // this code should be generic so it can be reused in future projects
        chain_data.update_from_websocket(message.clone());

        // specific program logic using the mirrored data
        match message {
            websocket_source::Message::Account(account_write) => {
                // TODO: Do we need to check health when open orders accounts change?
                if account_write.account.owner != mango_program_id || account_write.account.data.len() == 0 {
                    continue;
                }

                let kind = account_write.account.data[0];
                match kind {
                    // MangoAccount
                    1 => {
                        // TODO: Check that it belongs to the right group
                        // TODO: track mango account pubkeys in a set - need to iterate them for health checks
                        // TODO: check health of this particular one
                    },
                    // MangoCache
                    7 => {
                        // TODO: Check that it belongs to the right group, for that, we need to check the MangoGroup account
                        // TODO: check health of all accounts
                    },
                    _ => {},
                }
            },
            _ => {}
        }
    }
}
