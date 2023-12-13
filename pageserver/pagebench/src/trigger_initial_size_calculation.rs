use std::sync::Arc;

use tokio::task::{JoinError, JoinSet};

use crate::util::tenant_timeline_id::TenantTimelineId;

#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "localhost:64000")]
    page_service_host_port: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long, help = "if specified, poll mgmt api to check whether init logical size calculation has completed")]
    poll_for_completion: Option<Duration>,

    targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    let _guard = logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let main_task = rt.spawn(main_impl(args));
    rt.block_on(main_task).unwrap()
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver::client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        None, // TODO: support jwt in args
    ));

    // discover targets
    let mut timelines: Vec<TenantTimelineId> = Vec::new();
    if args.targets.is_some() {
        timelines = args.targets.clone().unwrap();
    } else {
        let tenants: Vec<TenantId> = mgmt_api_client
            .list_tenants()
            .await?
            .into_iter()
            .map(|ti| ti.id)
            .collect();
        let mut js = JoinSet::new();
        for tenant_id in tenants {
            js.spawn({
                let mgmt_api_client = Arc::clone(&mgmt_api_client);
                async move {
                    (
                        tenant_id,
                        mgmt_api_client.tenant_details(tenant_id).await.unwrap(),
                    )
                }
            });
        }
        while let Some(res) = js.join_next().await {
            let (tenant_id, details) = res.unwrap();
            for timeline_id in details.timelines {
                timelines.push(TenantTimelineId {
                    tenant_id,
                    timeline_id,
                });
            }
        }
    }

    info!("timelines:\n{:?}", timelines);

    // kick it off

    let mut js = JoinSet::new();
    for tl in timelines {
        let mgmt_api_client = Arc::clone(&mgmt_api_client);
        js.spawn(async move {
            // TODO: API to explicitly trigger initial logical size computation
            let mut info = mgmt_api_client
                .timeline_info(tl.tenant_id, tl.timeline_id)
                .await.unwrap();

            if let Some(period) = args.poll_for_completion {
                let mut ticker = tokio::time::interval(period);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay)
                while info.current_logical_size{

                    ticker.tick().await;
                    mgmt_api_client.timeline_info(tenant_id, timeline_id)
                }
            }




        })
    }
    while let Some(res) = js.join_next().await {
        let _: () = res.unwrap();
    }
}
