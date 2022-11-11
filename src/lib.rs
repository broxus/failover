#![warn(
    clippy::all,
    clippy::dbg_macro,
    clippy::todo,
    clippy::empty_enum,
    clippy::enum_glob_use,
    clippy::mem_forget,
    clippy::unused_self,
    clippy::filter_map_next,
    clippy::needless_continue,
    clippy::needless_borrow,
    clippy::match_wildcard_for_single_variants,
    clippy::if_let_mutex,
    clippy::mismatched_target_os,
    clippy::await_holding_lock,
    clippy::match_on_vec_items,
    clippy::imprecise_flops,
    clippy::suboptimal_flops,
    clippy::lossy_float_literal,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::fn_params_excessive_bools,
    clippy::exit,
    clippy::inefficient_to_string,
    clippy::linkedlist,
    clippy::macro_use_imports,
    clippy::option_option,
    clippy::verbose_file_reads,
    clippy::unnested_or_patterns,
    clippy::str_to_string,
    rust_2018_idioms,
    future_incompatible,
    nonstandard_style,
    missing_debug_implementations,
    clippy::unused_async,
    clippy::await_holding_lock
)]
#![deny(unreachable_pub, private_in_public)]
#![allow(elided_lifetimes_in_paths, clippy::type_complexity)]

use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use etcd_client::{Client, ConnectOptions, Error, LeaseClient, LeaseGrantOptions};
use futures::StreamExt;
use rand::Rng;
use tokio::time;
use tokio_util::sync::CancellationToken;

pub use etcd_client;

#[derive(Debug)]
pub struct Config {
    pub etcd_options: Option<ConnectOptions>,
    pub prefix: String,
    pub lease_time: Duration,
}

#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeState>,
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consensus")
            .field("prefix", &self.inner.prefix)
            .field("lease_id", &self.inner.lease_id)
            .field("peer_name", &self.inner.peer_name)
            .finish()
    }
}

impl Node {
    pub async fn new<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        peer_name: &str,
        config: Config,
    ) -> Result<Self> {
        let etcd_client = Client::connect(endpoints, config.etcd_options).await?;

        // receive lease id
        let mut lease_client = etcd_client.lease_client();
        let grant = lease_client
            .grant(config.lease_time.as_secs() as i64, None)
            .await
            .context("initial lease failed")?;
        let lease_id = LeaseId(grant.id());

        tracing::info!(
            %lease_id,
            lease_time = %grant.ttl(),
            "leased successfully"
        );

        let inner = Arc::new(NodeState {
            etcd_client,
            prefix: config.prefix,
            peer_name: peer_name.to_owned(),
            electors: Default::default(),
            previously_committed: Default::default(),
            lease_id,
            cancellation_token: CancellationToken::new(),
        });

        inner.start_heartbeat_process(config.lease_time).await?;

        Ok(Self { inner })
    }

    /// Commits new seqno for the specified shard
    pub async fn commit(&self, wc: i32, shard: u64, seq_no: u32) -> Result<bool> {
        let start = time::Instant::now();
        let res = self.inner.commit(ShardId { wc, shard }, seq_no).await;
        let duration = start.elapsed();
        if duration.as_secs() > 2 {
            tracing::warn!(?duration, wc, shard, seq_no, "long commit");
        }
        res
    }

    /// Returns seqno if you are a master for the specified shard
    pub async fn get_committed(&self, wc: i32, shard: u64) -> Result<Option<u32>> {
        self.inner.get_committed(ShardId { wc, shard }).await
    }
}

struct NodeState {
    etcd_client: Client,
    prefix: String,
    peer_name: String,
    electors: DashMap<ShardId, Arc<ShardClient>>,
    previously_committed: DashMap<ShardId, u32>,
    lease_id: LeaseId,
    cancellation_token: CancellationToken,
}

impl NodeState {
    /// A background process which proves that you are alive
    async fn start_heartbeat_process(&self, lease_time: Duration) -> Result<()> {
        async fn lease_round(client: &mut LeaseClient, lease_time: Duration, lease_id: LeaseId) {
            let retry_interval = Duration::from_secs(1);

            let half_lease_time = lease_time / 2;

            // getting lease with previously assigned id
            let grant_options = LeaseGrantOptions::new().with_id(*lease_id);
            match client
                .grant(lease_time.as_secs() as i64, Some(grant_options))
                .await
            {
                Ok(_) => {
                    tracing::debug!(%lease_id, "renewed lease");
                }
                Err(Error::GRpcStatus(s))
                    if s.code() == tonic::Code::FailedPrecondition
                        && s.message().contains("lease already exists") =>
                {
                    tracing::debug!(%lease_id, "lease already exists");
                }
                Err(e) => {
                    if !matches!(e, Error::GRpcStatus(_)) {
                        tracing::error!("lease grant error: {e:?}");
                    }

                    time::sleep(retry_interval).await;
                    return;
                }
            }

            // -----------------------------------------------------
            // keep alive stream
            let (mut keeper, mut stream) = match client.keep_alive(*lease_id).await {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!("failed to get lease keep alive stream: {e:?}");
                    return;
                }
            };

            // polling keep alive stream answers
            tokio::spawn(async move {
                while let Some(keep_alive) = stream.next().await {
                    if let Err(e) = keep_alive {
                        tracing::error!("lease keep alive received stream error: {e:?}");
                    }
                }
            });
            // end of keep alive stream
            // -----------------------------------------------------

            // main keep alive loop
            loop {
                let now = std::time::Instant::now();
                // trying to prolong lease

                while let Err(e) = keeper.keep_alive().await {
                    tracing::error!("lease keep alive error: {e:?}");
                    if now.elapsed() > lease_time {
                        return;
                    }
                    time::sleep(retry_interval).await;
                }
                tracing::debug!("lease keep alive success");

                let elapsed = now.elapsed();
                if elapsed > lease_time {
                    tracing::warn!("restarting main keep alive loop because lease expired");
                    return;
                } else if elapsed < half_lease_time {
                    time::sleep(half_lease_time - elapsed).await;
                }
            }
        }

        let mut lease_client = self.etcd_client.lease_client();
        let cancellation_token = self.cancellation_token.clone();
        let lease_id = self.lease_id;

        // trying  to keep lease alive
        tokio::spawn(async move {
            tokio::pin!(let cancelled = cancellation_token.cancelled(););
            loop {
                tokio::select! {
                    _ = lease_round(&mut lease_client, lease_time, lease_id) => {
                        tracing::warn!("restarted lease round");
                    }
                    _ = &mut cancelled => {
                        tracing::warn!("lease task cancelled");
                        return;
                    }
                }
            }
        });

        Ok(())
    }

    /// Commits new seqno for the specified shard
    async fn commit(&self, shard_id: ShardId, seq_no: u32) -> Result<bool> {
        let elector = self.get_elector(shard_id).await?;

        let mut kv_client = self.etcd_client.kv_client();
        let key = shard_id.prefixed(&self.prefix);

        loop {
            if elector.is_leader() {
                while let Err(e) = kv_client
                    .put(key.to_string(), seq_no.to_string(), None)
                    .await
                {
                    tracing::error!(%shard_id, %key, value = seq_no, "failed to commit a value: {e:?}");
                    time::sleep(Duration::from_secs(1)).await;
                }

                *self.previously_committed.entry(shard_id).or_default() = seq_no;
                return Ok(true);
            }

            // todo: add fast_path option
            let previous_value = self
                .previously_committed
                .get(&shard_id)
                .map(|r| *r.value())
                .unwrap_or(0);
            if previous_value >= seq_no {
                return Ok(false);
            }

            // Request existing value
            let value = kv_client.get(key.to_string(), None).await?;
            let Some(kv) = value.kvs().first() else { continue; };

            let Ok(stored) = std::str::from_utf8(kv.value()) else {
                tracing::warn!(%key, "invalid etcd value");
                time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            let Ok(stored) = u32::from_str(stored) else {
                tracing::warn!(%key, value = stored, "invalid etcd value");
                time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            *self.previously_committed.entry(shard_id).or_default() = stored;

            // mitigate case, when leader committed seq_no before death but haven't yet written actual data
            if stored > seq_no + 1 {
                break;
            }

            time::sleep(Duration::from_secs(1)).await;
        }

        Ok(false)
    }

    async fn get_elector(&self, shard_id: ShardId) -> Result<Arc<ShardClient>> {
        if let Some(elector) = self.electors.get(&shard_id) {
            return Ok(elector.value().clone());
        }

        tracing::info!(%shard_id, "creating new elector");

        let cancel_token = self.cancellation_token.child_token();
        let elector = ShardClient::new(
            self.etcd_client.clone(),
            self.lease_id,
            shard_id,
            self.peer_name.clone(),
            cancel_token,
        )
        .await?;

        self.electors.insert(shard_id, elector.clone());
        Ok(elector)
    }

    async fn get_committed(&self, shard_id: ShardId) -> Result<Option<u32>> {
        let key = shard_id.prefixed(&self.prefix);

        let mut kv_client = self.etcd_client.kv_client();

        let value = kv_client.get(key.to_string(), None).await?;
        let Some(kv) = value.kvs().first() else {
            return Ok(None);
        };

        let Ok(stored) = std::str::from_utf8(kv.value()) else {
            tracing::warn!(%key, "invalid etcd value");
            return Ok(None);
        };

        let Ok(seq_no) = u32::from_str(stored) else {
            tracing::warn!(%key, value = stored, "invalid etcd value");
            return Ok(None);
        };

        Ok(Some(seq_no))
    }
}

impl Drop for NodeState {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

struct ShardClient {
    is_leader: Arc<AtomicBool>,
    client: Client,
    lease_id: LeaseId,
    shard_id: ShardId,
    name: String,
}

impl ShardClient {
    async fn new(
        client: Client,
        lease_id: LeaseId,
        shard_id: ShardId,
        name: String,
        cancel_token: CancellationToken,
    ) -> Result<Arc<Self>> {
        let client = Arc::new(Self {
            is_leader: Default::default(), // init later
            client,
            lease_id,
            shard_id,
            name,
        });

        let is_leader = client.try_elect().await.context("Failed to elect")?;
        client.is_leader.store(is_leader, Ordering::Release);

        tokio::spawn({
            let client = client.clone();
            async move {
                tokio::pin!(let cancelled = cancel_token.cancelled(););

                loop {
                    tokio::select! {
                        res = client.status_observer() => {
                            tracing::warn!("observer stopped with result: {res:?}");
                        }
                        _ = &mut cancelled => {
                            tracing::warn!("observation cancelled");
                            return;
                        }
                    }
                }
            }
        });

        Ok(client)
    }

    async fn status_observer(&self) -> Result<()> {
        let campaign = self.shard_id.to_string();
        let mut observer = self.client.clone().observe(campaign).await?;

        let leader_self = self.lease_id.make_leader_value(&self.name);
        let value = leader_self.to_string().into_bytes();

        while let Some(event) = observer.next().await {
            let mut status = match event {
                Ok(status) => status,
                Err(e) => {
                    tracing::error!("election status observer error: {e:?}");
                    continue;
                }
            };

            let (mut is_leader, leader_lease_id) = match status.take_kv() {
                Some(kv) => (kv.value() == value, Some(LeaseId(kv.lease()))),
                None => {
                    tracing::info!("leader is dead, trying to elect");
                    (false, None)
                }
            };

            if !is_leader {
                if let Some(leader_lease_id) = leader_lease_id {
                    if let Err(e) = self.wait_for_leader_death(leader_lease_id).await {
                        tracing::error!(%leader_lease_id, "failed to wait for leader death: {e:?}");
                    }
                }

                is_leader = loop {
                    match self.try_elect().await {
                        Ok(is_leader) => break is_leader,
                        Err(e) => {
                            tracing::error!("failed to elect: {e:?}");
                            time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                };
            }

            self.is_leader.store(is_leader, Ordering::Release);
            if is_leader {
                tracing::info!("we became a leader");
            }
        }

        Ok(())
    }

    async fn try_elect(&self) -> Result<bool> {
        let campaign = self.shard_id.to_string();

        let leader_self = self.lease_id.make_leader_value(&self.name);
        let value = leader_self.to_string().into_bytes();

        let waiting_time_range = Duration::from_secs(1)..Duration::from_secs(10);
        let mut election_client = self.client.election_client();

        Ok(loop {
            tracing::info!(
                lease_id = %self.lease_id,
                campaign = %self.shard_id,
                %leader_self,
                "trying to become a leader"
            );

            let rand_wait_time = rand::thread_rng().gen_range(waiting_time_range.clone());
            let campaign_fut =
                election_client.campaign(campaign.clone(), value.clone(), *self.lease_id);

            let Ok(campaign_res) = time::timeout(rand_wait_time, campaign_fut).await else {
                tracing::info!("there is another leader, giving up election");
                match election_client.leader(campaign.clone()).await {
                    Ok(res) => break matches!(res.kv(), Some(kv) if kv.value() == value),
                    Err(e) => {
                        tracing::error!("failed to get leader: {e:?}");
                        time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };

            // call blocks, so if we are waiting longer than n secs than there is another leader. Or not.
            match campaign_res {
                Ok(mut response) => {
                    tracing::debug!(?response, "got campaign response");
                    break matches!(response.take_leader(), Some(leader) if leader.lease() == *self.lease_id);
                }
                // etcd fucked up and returned an error.
                Err(e) => {
                    tracing::error!("failed to become a leader: {e:?}");
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
        })
    }

    async fn wait_for_leader_death(&self, leader_lease_id: LeaseId) -> Result<()> {
        let mut lease_client = self.client.lease_client();
        loop {
            match lease_client.time_to_live(*leader_lease_id, None).await {
                Ok(res) if res.ttl() < 0 => {
                    return Ok(());
                }
                Ok(res) => {
                    time::sleep(Duration::from_secs(res.ttl() as u64 + 1)).await;
                }
                Err(
                    e @ (etcd_client::Error::IoError(_) | etcd_client::Error::TransportError(_)),
                ) => {
                    tracing::warn!("retrying lease client ttl after error: {e:?}");
                    // TODO: move into config
                    time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => break Err(e.into()),
            }
        }
    }

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct ShardId {
    wc: i32,
    shard: u64,
}

impl ShardId {
    fn prefixed(self, prefix: &str) -> PrefixedShardId<'_> {
        PrefixedShardId {
            prefix,
            shard_id: self,
        }
    }
}

impl Display for ShardId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{:016x}", self.wc, self.shard))
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct PrefixedShardId<'a> {
    prefix: &'a str,
    shard_id: ShardId,
}

impl Display for PrefixedShardId<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}/{}", self.prefix, self.shard_id))
    }
}

#[derive(Clone, Copy)]
struct LeaderValue<'a> {
    name: &'a str,
    lease_id: LeaseId,
}

impl Display for LeaderValue<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}/{}", self.name, self.lease_id))
    }
}

#[derive(Clone, Copy)]
struct LeaseId(i64);

impl LeaseId {
    fn make_leader_value(self, name: &str) -> LeaderValue<'_> {
        LeaderValue {
            name,
            lease_id: self,
        }
    }
}

impl std::ops::Deref for LeaseId {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for LeaseId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:#4x}", self.0))
    }
}

impl Debug for LeaseId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
