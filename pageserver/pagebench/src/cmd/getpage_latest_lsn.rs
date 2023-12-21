use anyhow::Context;
use futures::future::join_all;
use pageserver::pgdatadir_mapping::key_to_rel_block;
use pageserver::repository;
use pageserver_api::key::is_rel_block_key;
use pageserver_client::page_service::RelTagBlockNo;

use utils::id::TenantTimelineId;
use utils::lsn::Lsn;

use rand::prelude::*;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tracing::{info, instrument};

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::util::tokio_thread_local_stats::AllThreadLocalStats;
use crate::util::{request_stats, tokio_thread_local_stats};

/// GetPage@LatestLSN, uniformly distributed across the compute-accessible keyspace.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
    #[clap(long)]
    per_target_rate_limit: Option<usize>,
    #[clap(long)]
    limit_to_first_n_targets: Option<usize>,
    targets: Option<Vec<TenantTimelineId>>,
}

#[derive(Debug, Default)]
struct LiveStats {
    completed_requests: AtomicU64,
}

impl LiveStats {
    fn inc(&self) {
        self.completed_requests.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct KeyRange {
    timeline: TenantTimelineId,
    timeline_lsn: Lsn,
    start: i128,
    end: i128,
}

impl KeyRange {
    fn len(&self) -> i128 {
        self.end - self.start
    }
}

#[derive(serde::Serialize)]
struct Output {
    total: request_stats::Output,
}

tokio_thread_local_stats::declare!(STATS: request_stats::Stats);

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    tokio_thread_local_stats::main!(STATS, move |thread_local_stats| {
        main_impl(args, thread_local_stats)
    })
}

async fn main_impl(
    args: Args,
    all_thread_local_stats: AllThreadLocalStats<request_stats::Stats>,
) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    // discover targets
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: args.limit_to_first_n_targets,
            targets: args.targets.clone(),
        },
    )
    .await?;

    let mut js = JoinSet::new();
    for timeline in &timelines {
        js.spawn({
            let mgmt_api_client = Arc::clone(&mgmt_api_client);
            let timeline = *timeline;
            async move {
                let partitioning = mgmt_api_client
                    .keyspace(timeline.tenant_id, timeline.timeline_id)
                    .await?;
                let lsn = partitioning.at_lsn;

                let ranges = partitioning
                    .keys
                    .ranges
                    .iter()
                    .filter_map(|r| {
                        let start = r.start;
                        let end = r.end;
                        // filter out non-relblock keys
                        match (is_rel_block_key(&start), is_rel_block_key(&end)) {
                            (true, true) => Some(KeyRange {
                                timeline,
                                timeline_lsn: lsn,
                                start: start.to_i128(),
                                end: end.to_i128(),
                            }),
                            (true, false) | (false, true) => {
                                unimplemented!("split up range")
                            }
                            (false, false) => None,
                        }
                    })
                    .collect::<Vec<_>>();

                anyhow::Ok(ranges)
            }
        });
    }
    let mut all_ranges: Vec<KeyRange> = Vec::new();
    while let Some(res) = js.join_next().await {
        all_ranges.extend(res.unwrap().unwrap());
    }

    let live_stats = Arc::new(LiveStats::default());

    let num_client_tasks = timelines.len();
    let num_live_stats_dump = 1;
    let num_work_sender_tasks = 1;

    let start_work_barrier = Arc::new(tokio::sync::Barrier::new(
        num_client_tasks + num_live_stats_dump + num_work_sender_tasks,
    ));
    let all_work_done_barrier = Arc::new(tokio::sync::Barrier::new(num_client_tasks));

    tokio::spawn({
        let stats = Arc::clone(&live_stats);
        let start_work_barrier = Arc::clone(&start_work_barrier);
        async move {
            start_work_barrier.wait().await;
            loop {
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let completed_requests = stats.completed_requests.swap(0, Ordering::Relaxed);
                let elapsed = start.elapsed();
                info!(
                    "RPS: {:.0}",
                    completed_requests as f64 / elapsed.as_secs_f64()
                );
            }
        }
    });

    let mut work_senders = HashMap::new();
    let mut tasks = Vec::new();
    for tl in &timelines {
        let (sender, receiver) = tokio::sync::mpsc::channel(10); // TODO: not sure what the implications of this are
        work_senders.insert(tl, sender);
        tasks.push(tokio::spawn(client(
            args,
            *tl,
            Arc::clone(&start_work_barrier),
            receiver,
            Arc::clone(&all_work_done_barrier),
            Arc::clone(&live_stats),
        )));
    }

    let work_sender: Pin<Box<dyn Send + Future<Output = ()>>> = match args.per_target_rate_limit {
        None => Box::pin(async move {
            let weights = rand::distributions::weighted::WeightedIndex::new(
                all_ranges.iter().map(|v| v.len()),
            )
            .unwrap();

            start_work_barrier.wait().await;

            loop {
                let (range, key) = {
                    let mut rng = rand::thread_rng();
                    let r = &all_ranges[weights.sample(&mut rng)];
                    let key: i128 = rng.gen_range(r.start..r.end);
                    let key = repository::Key::from_i128(key);
                    let (rel_tag, block_no) =
                        key_to_rel_block(key).expect("we filter non-rel-block keys out above");
                    (r, RelTagBlockNo { rel_tag, block_no })
                };
                let sender = work_senders.get(&range.timeline).unwrap();
                // TODO: what if this blocks?
                sender.send((key, range.timeline_lsn)).await.ok().unwrap();
            }
        }),
        Some(rps_limit) => Box::pin(async move {
            let period = Duration::from_secs_f64(1.0 / (rps_limit as f64));

            let make_timeline_task: &dyn Fn(
                TenantTimelineId,
            )
                -> Pin<Box<dyn Send + Future<Output = ()>>> = &|timeline| {
                let sender = work_senders.get(&timeline).unwrap();
                let ranges: Vec<KeyRange> = all_ranges
                    .iter()
                    .filter(|r| r.timeline == timeline)
                    .cloned()
                    .collect();
                let weights = rand::distributions::weighted::WeightedIndex::new(
                    ranges.iter().map(|v| v.len()),
                )
                .unwrap();

                Box::pin(async move {
                    let mut ticker = tokio::time::interval(period);
                    ticker.set_missed_tick_behavior(
                        /* TODO review this choice */
                        tokio::time::MissedTickBehavior::Burst,
                    );
                    loop {
                        ticker.tick().await;
                        let (range, key) = {
                            let mut rng = rand::thread_rng();
                            let r = &ranges[weights.sample(&mut rng)];
                            let key: i128 = rng.gen_range(r.start..r.end);
                            let key = repository::Key::from_i128(key);
                            let (rel_tag, block_no) = key_to_rel_block(key)
                                .expect("we filter non-rel-block keys out above");
                            (r, RelTagBlockNo { rel_tag, block_no })
                        };
                        sender.send((key, range.timeline_lsn)).await.ok().unwrap();
                    }
                })
            };

            let tasks: Vec<_> = work_senders
                .keys()
                .map(|tl| make_timeline_task(**tl))
                .collect();

            start_work_barrier.wait().await;

            join_all(tasks).await;
        }),
    };

    if let Some(runtime) = args.runtime {
        match tokio::time::timeout(runtime.into(), work_sender).await {
            Ok(()) => unreachable!("work sender never terminates"),
            Err(_timeout) => {
                // this implicitly drops the work_senders, making all the clients exit
            }
        }
    } else {
        work_sender.await;
        unreachable!("work sender never terminates");
    }

    for t in tasks {
        t.await.unwrap();
    }

    let output = Output {
        total: {
            let mut agg_stats = request_stats::Stats::new();
            for stats in all_thread_local_stats.lock().unwrap().iter() {
                let stats = stats.lock().unwrap();
                agg_stats.add(&stats);
            }
            agg_stats.output()
        },
    };

    let output = serde_json::to_string_pretty(&output).unwrap();
    println!("{output}");

    anyhow::Ok(())
}

#[instrument(skip_all)]
async fn client(
    args: &'static Args,
    timeline: TenantTimelineId,
    start_work_barrier: Arc<Barrier>,
    mut work: tokio::sync::mpsc::Receiver<(RelTagBlockNo, Lsn)>,
    all_work_done_barrier: Arc<Barrier>,
    live_stats: Arc<LiveStats>,
) {
    start_work_barrier.wait().await;

    let client = pageserver_client::page_service::Client::new(args.page_service_connstring.clone())
        .await
        .unwrap();
    let mut client = client
        .pagestream(timeline.tenant_id, timeline.timeline_id)
        .await
        .unwrap();

    while let Some((key, lsn)) = work.recv().await {
        let start = Instant::now();
        client
            .getpage(key, lsn)
            .await
            .with_context(|| format!("getpage for {timeline}"))
            .unwrap();
        let elapsed = start.elapsed();
        live_stats.inc();
        STATS.with(|stats| {
            stats.borrow().lock().unwrap().observe(elapsed).unwrap();
        });
    }

    all_work_done_barrier.wait().await;
}
