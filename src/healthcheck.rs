use {
    crate::chain_data::ChainData,
    crate::websocket_sink::LiquidatableInfo,
    anyhow::Context,
    fixed::types::I80F48,
    log::*,
    mango::state::{
        DataType, HealthCache, HealthType, MangoAccount, MangoCache, MangoGroup, UserActiveAssets,
        MAX_PAIRS,
    },
    mango_common::Loadable,
    solana_sdk::account::{AccountSharedData, ReadableAccount},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashSet,
    tokio::sync::broadcast,
};

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

fn load_mango_account_from_chain<'a, T: Loadable + Sized>(
    data_type: DataType,
    chain_data: &'a ChainData,
    pubkey: &Pubkey,
) -> anyhow::Result<&'a T> {
    load_mango_account::<T>(
        data_type,
        chain_data
            .account(pubkey)
            .context("retrieving account from chain")?,
    )
}

pub fn load_open_orders_account(
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
            unpacked[i] = Some(load_open_orders_account(oo)?);
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

fn compute_liquidatable(
    group: &MangoGroup,
    cache: &MangoCache,
    account: &MangoAccount,
    open_orders: &Vec<Option<&serum_dex::state::OpenOrders>>,
) -> anyhow::Result<IsLiquidatable> {
    let assets = UserActiveAssets::new(group, account, vec![]);
    let mut health_cache = HealthCache::new(assets);
    health_cache.init_vals_with_orders_vec(group, cache, account, open_orders)?;

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

pub fn process_accounts<'a>(
    chain_data: &ChainData,
    group_id: &Pubkey,
    cache_id: &Pubkey,
    accounts: impl Iterator<Item = &'a Pubkey>,
    currently_liquidatable: &mut HashSet<Pubkey>,
    tx: &broadcast::Sender<LiquidatableInfo>,
) -> anyhow::Result<()> {
    let group =
        load_mango_account_from_chain::<MangoGroup>(DataType::MangoGroup, chain_data, group_id)
            .context("loading group account")?;
    let cache =
        load_mango_account_from_chain::<MangoCache>(DataType::MangoCache, chain_data, cache_id)
            .context("loading cache account")?;

    for pubkey in accounts {
        let account_result = load_mango_account_from_chain::<MangoAccount>(
            DataType::MangoAccount,
            chain_data,
            pubkey,
        );
        let account = match account_result {
            Ok(account) => account,
            Err(err) => {
                warn!("could not load account {}: {:?}", pubkey, err);
                continue;
            }
        };
        let oos = match get_open_orders(chain_data, group, account) {
            Ok(oos) => oos,
            Err(err) => {
                warn!("could not load account {} open orders: {:?}", pubkey, err);
                continue;
            }
        };

        let liquidatable = match compute_liquidatable(group, cache, account, &oos) {
            Ok(d) => d.liquidatable,
            Err(err) => {
                warn!("error computing health of {}: {:?}", pubkey, err);
                continue;
            }
        };

        let was_liquidatable = currently_liquidatable.contains(pubkey);
        if liquidatable && !was_liquidatable {
            info!("account {} is newly liquidatable", pubkey);
            currently_liquidatable.insert(pubkey.clone());
            let _ = tx.send(LiquidatableInfo::Start {
                account: pubkey.clone(),
            });
        }
        if liquidatable {
            let _ = tx.send(LiquidatableInfo::Now {
                account: pubkey.clone(),
            });
        }
        if !liquidatable && was_liquidatable {
            info!("account {} stopped being liquidatable", pubkey);
            currently_liquidatable.remove(pubkey);
            let _ = tx.send(LiquidatableInfo::Stop {
                account: pubkey.clone(),
            });
        }
    }

    Ok(())
}
