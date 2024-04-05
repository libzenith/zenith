use std::{collections::HashMap, str::FromStr};

use clap::{Parser, Subcommand};
use hyper::Method;
use pageserver_api::{
    controller_api::{
        NodeAvailabilityWrapper, NodeDescribeResponse, ShardSchedulingPolicy,
        TenantDescribeResponse, TenantPolicyRequest,
    },
    models::{
        ShardParameters, TenantConfig, TenantConfigRequest, TenantCreateRequest,
        TenantShardSplitRequest, TenantShardSplitResponse,
    },
    shard::{ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api::{self, ResponseErrorMessageExt};
use reqwest::Url;
use serde::{de::DeserializeOwned, Serialize};
use utils::id::{NodeId, TenantId};

use pageserver_api::controller_api::{
    NodeConfigureRequest, NodeRegisterRequest, NodeSchedulingPolicy, PlacementPolicy,
    TenantLocateResponse, TenantShardMigrateRequest, TenantShardMigrateResponse,
};

#[derive(Subcommand, Debug)]
enum Command {
    /// Register a pageserver with the storage controller.  This shouldn't usually be necessary,
    /// since pageservers auto-register when they start up
    NodeRegister {
        #[arg(long)]
        node_id: NodeId,

        #[arg(long)]
        listen_pg_addr: String,
        #[arg(long)]
        listen_pg_port: u16,

        #[arg(long)]
        listen_http_addr: String,
        #[arg(long)]
        listen_http_port: u16,
    },

    /// Modify a node's configuration in the storage controller
    NodeConfigure {
        #[arg(long)]
        node_id: NodeId,

        /// Availability is usually auto-detected based on heartbeats.  Set 'offline' here to
        /// manually mark a node offline
        #[arg(long)]
        availability: Option<NodeAvailabilityArg>,
        /// Scheduling policy controls whether tenant shards may be scheduled onto this node.
        #[arg(long)]
        scheduling: Option<NodeSchedulingPolicy>,
    },
    /// Modify a tenant's policies in the storage controller
    TenantPolicy {
        #[arg(long)]
        tenant_id: TenantId,
        /// Placement policy controls whether a tenant is `detached`, has only a secondary location (`secondary`),
        /// or is in the normal attached state with N secondary locations (`attached:N`)
        #[arg(long)]
        placement: Option<PlacementPolicyArg>,
        /// Scheduling policy enables pausing the controller's scheduling activity involving this tenant.  `active` is normal,
        /// `essential` disables optimization scheduling changes, `pause` disables all scheduling changes, and `stop` prevents
        /// all reconciliation activity including for scheduling changes already made.  `pause` and `stop` can make a tenant
        /// unavailable, and are only for use in emergencies.
        #[arg(long)]
        scheduling: Option<ShardSchedulingPolicyArg>,
    },
    /// List nodes known to the storage controller
    Nodes {},
    /// List tenants known to the storage controller
    Tenants {},
    /// Create a new tenant in the storage controller, and by extension on pageservers.
    TenantCreate {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Delete a tenant in the storage controller, and by extension on pageservers.
    TenantDelete {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Split an existing tenant into a higher number of shards than its current shard count.
    TenantShardSplit {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        shard_count: u8,
        /// Optional, in 8kiB pages.  e.g. set 2048 for 16MB stripes.
        #[arg(long)]
        stripe_size: Option<u32>,
    },
    /// Migrate the attached location for a tenant shard to a specific pageserver.
    TenantShardMigrate {
        #[arg(long)]
        tenant_shard_id: TenantShardId,
        #[arg(long)]
        node: NodeId,
    },
    /// Modify the pageserver tenant configuration of a tenant: this is the configuration structure
    /// that is passed through to pageservers, and does not affect storage controller behavior.
    TenantConfig {
        #[arg(long)]
        tenant_id: TenantId,
        #[arg(long)]
        config: String,
    },
    /// Attempt to balance the locations for a tenant across pageservers.  This is a client-side
    /// alternative to the storage controller's scheduling optimization behavior.
    TenantScatter {
        #[arg(long)]
        tenant_id: TenantId,
    },
    /// Print details about a particular tenant, including all its shards' states.
    TenantDescribe {
        #[arg(long)]
        tenant_id: TenantId,
    },
}

#[derive(Parser)]
#[command(
    author,
    version,
    about,
    long_about = "CLI for Storage Controller Support/Debug"
)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[arg(long)]
    /// URL to storage controller.  e.g. http://127.0.0.1:1234 when using `neon_local`
    api: Url,

    #[arg(long)]
    /// JWT token for authenticating with storage controller.  Depending on the API used, this
    /// should have either `pageserverapi` or `admin` scopes: for convenience, you should mint
    /// a token with both scopes to use with this tool.
    jwt: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone)]
struct PlacementPolicyArg(PlacementPolicy);

impl FromStr for PlacementPolicyArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "detached" => Ok(Self(PlacementPolicy::Detached)),
            "secondary" => Ok(Self(PlacementPolicy::Secondary)),
            _ if s.starts_with("attached:") => {
                let mut splitter = s.split(':');
                let _prefix = splitter.next().unwrap();
                match splitter.next().and_then(|s| s.parse::<usize>().ok()) {
                    Some(n) => Ok(Self(PlacementPolicy::Attached(n))),
                    None => Err(anyhow::anyhow!(
                        "Invalid format '{s}', a valid example is 'attached:1'"
                    )),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Unknown placement policy '{s}', try detached,secondary,attached:<n>"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct ShardSchedulingPolicyArg(ShardSchedulingPolicy);

impl FromStr for ShardSchedulingPolicyArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self(ShardSchedulingPolicy::Active)),
            "essential" => Ok(Self(ShardSchedulingPolicy::Essential)),
            "pause" => Ok(Self(ShardSchedulingPolicy::Pause)),
            "stop" => Ok(Self(ShardSchedulingPolicy::Stop)),
            _ => Err(anyhow::anyhow!(
                "Unknown scheduling policy '{s}', try active,essential,pause,stop"
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct NodeAvailabilityArg(NodeAvailabilityWrapper);

impl FromStr for NodeAvailabilityArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self(NodeAvailabilityWrapper::Active)),
            "offline" => Ok(Self(NodeAvailabilityWrapper::Offline)),
            _ => Err(anyhow::anyhow!("Unknown availability state '{s}'")),
        }
    }
}

struct Client {
    base_url: Url,
    jwt_token: Option<String>,
    client: reqwest::Client,
}

impl Client {
    fn new(base_url: Url, jwt_token: Option<String>) -> Self {
        Self {
            base_url,
            jwt_token,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
        }
    }

    /// Simple HTTP request wrapper for calling into storage controller
    async fn dispatch<RQ, RS>(
        &self,
        method: hyper::Method,
        path: String,
        body: Option<RQ>,
    ) -> mgmt_api::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        // The configured URL has the /upcall path prefix for pageservers to use: we will strip that out
        // for general purpose API access.
        let url = Url::from_str(&format!(
            "http://{}:{}/{path}",
            self.base_url.host_str().unwrap(),
            self.base_url.port().unwrap()
        ))
        .unwrap();

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }
        if let Some(jwt_token) = &self.jwt_token {
            builder = builder.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {jwt_token}"),
            );
        }

        let response = builder.send().await.map_err(mgmt_api::Error::ReceiveBody)?;
        let response = response.error_from_body().await?;

        response
            .json()
            .await
            .map_err(pageserver_client::mgmt_api::Error::ReceiveBody)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let storcon_client = Client::new(cli.api.clone(), cli.jwt.clone());

    let mut trimmed = cli.api.to_string();
    trimmed.pop();
    let vps_client = mgmt_api::Client::new(trimmed, cli.jwt.as_deref());

    match cli.command {
        Command::NodeRegister {
            node_id,
            listen_pg_addr,
            listen_pg_port,
            listen_http_addr,
            listen_http_port,
        } => {
            storcon_client
                .dispatch::<_, ()>(
                    Method::POST,
                    "control/v1/node".to_string(),
                    Some(NodeRegisterRequest {
                        node_id,
                        listen_pg_addr,
                        listen_pg_port,
                        listen_http_addr,
                        listen_http_port,
                    }),
                )
                .await?;
        }
        Command::TenantCreate { tenant_id } => {
            vps_client
                .tenant_create(&TenantCreateRequest {
                    new_tenant_id: TenantShardId::unsharded(tenant_id),
                    generation: None,
                    shard_parameters: ShardParameters::default(),
                    placement_policy: Some(PlacementPolicy::Attached(1)),
                    config: TenantConfig::default(),
                })
                .await?;
        }
        Command::TenantDelete { tenant_id } => {
            let status = vps_client
                .tenant_delete(TenantShardId::unsharded(tenant_id))
                .await?;
            tracing::info!("Delete status: {}", status);
        }
        Command::Nodes {} => {
            let resp = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;
            let mut table = comfy_table::Table::new();
            table.set_header(["Id", "Hostname", "Scheduling", "Availability"]);
            for node in resp {
                table.add_row([
                    format!("{}", node.id),
                    node.listen_http_addr,
                    format!("{:?}", node.scheduling),
                    format!("{:?}", node.availability),
                ]);
            }
            println!("{table}");
        }
        Command::NodeConfigure {
            node_id,
            availability,
            scheduling,
        } => {
            let req = NodeConfigureRequest {
                node_id,
                availability: availability.map(|a| a.0),
                scheduling,
            };
            storcon_client
                .dispatch::<_, ()>(
                    Method::PUT,
                    format!("control/v1/node/{node_id}/config"),
                    Some(req),
                )
                .await?;
        }
        Command::Tenants {} => {
            let resp = storcon_client
                .dispatch::<(), Vec<TenantDescribeResponse>>(
                    Method::GET,
                    "control/v1/tenant".to_string(),
                    None,
                )
                .await?;
            let mut table = comfy_table::Table::new();
            table.set_header([
                "TenantId",
                "ShardCount",
                "StripeSize",
                "Placement",
                "Scheduling",
            ]);
            for tenant in resp {
                let shard_zero = tenant.shards.into_iter().next().unwrap();
                table.add_row([
                    format!("{}", tenant.tenant_id),
                    format!("{}", shard_zero.tenant_shard_id.shard_count.literal()),
                    format!("{:?}", tenant.stripe_size),
                    format!("{:?}", tenant.policy),
                    format!("{:?}", shard_zero.scheduling_policy),
                ]);
            }

            println!("{table}");
        }
        Command::TenantPolicy {
            tenant_id,
            placement,
            scheduling,
        } => {
            let req = TenantPolicyRequest {
                scheduling: scheduling.map(|s| s.0),
                placement: placement.map(|p| p.0),
            };
            storcon_client
                .dispatch::<_, ()>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_id}/policy"),
                    Some(req),
                )
                .await?;
        }
        Command::TenantShardSplit {
            tenant_id,
            shard_count,
            stripe_size,
        } => {
            let req = TenantShardSplitRequest {
                new_shard_count: shard_count,
                new_stripe_size: stripe_size.map(ShardStripeSize),
            };

            let response = storcon_client
                .dispatch::<TenantShardSplitRequest, TenantShardSplitResponse>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_id}/shard_split"),
                    Some(req),
                )
                .await?;
            println!(
                "Split tenant {} into {} shards: {}",
                tenant_id,
                shard_count,
                response
                    .new_shards
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        Command::TenantShardMigrate {
            tenant_shard_id,
            node,
        } => {
            let req = TenantShardMigrateRequest {
                tenant_shard_id,
                node_id: node,
            };

            storcon_client
                .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                    Method::PUT,
                    format!("control/v1/tenant/{tenant_shard_id}/migrate"),
                    Some(req),
                )
                .await?;
        }
        Command::TenantConfig { tenant_id, config } => {
            let tenant_conf = serde_json::from_str(&config)?;

            vps_client
                .tenant_config(&TenantConfigRequest {
                    tenant_id,
                    config: tenant_conf,
                })
                .await?;
        }
        Command::TenantScatter { tenant_id } => {
            // Find the shards
            let locate_response = storcon_client
                .dispatch::<(), TenantLocateResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}/locate"),
                    None,
                )
                .await?;
            let shards = locate_response.shards;

            let mut node_to_shards: HashMap<NodeId, Vec<TenantShardId>> = HashMap::new();
            let shard_count = shards.len();
            for s in shards {
                let entry = node_to_shards.entry(s.node_id).or_default();
                entry.push(s.shard_id);
            }

            // Load list of available nodes
            let nodes_resp = storcon_client
                .dispatch::<(), Vec<NodeDescribeResponse>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;

            for node in nodes_resp {
                if matches!(node.availability, NodeAvailabilityWrapper::Active) {
                    node_to_shards.entry(node.id).or_default();
                }
            }

            let max_shard_per_node = shard_count / node_to_shards.len();

            loop {
                let mut migrate_shard = None;
                for shards in node_to_shards.values_mut() {
                    if shards.len() > max_shard_per_node {
                        // Pick the emptiest
                        migrate_shard = Some(shards.pop().unwrap());
                    }
                }
                let Some(migrate_shard) = migrate_shard else {
                    break;
                };

                // Pick the emptiest node to migrate to
                let mut destinations = node_to_shards
                    .iter()
                    .map(|(k, v)| (k, v.len()))
                    .collect::<Vec<_>>();
                destinations.sort_by_key(|i| i.1);
                let (destination_node, destination_count) = *destinations.first().unwrap();
                if destination_count + 1 > max_shard_per_node {
                    // Even the emptiest destination doesn't have space: we're done
                    break;
                }
                let destination_node = *destination_node;

                node_to_shards
                    .get_mut(&destination_node)
                    .unwrap()
                    .push(migrate_shard);

                println!("Migrate {} -> {} ...", migrate_shard, destination_node);

                storcon_client
                    .dispatch::<TenantShardMigrateRequest, TenantShardMigrateResponse>(
                        Method::PUT,
                        format!("control/v1/tenant/{migrate_shard}/migrate"),
                        Some(TenantShardMigrateRequest {
                            tenant_shard_id: migrate_shard,
                            node_id: destination_node,
                        }),
                    )
                    .await?;
                println!("Migrate {} -> {} OK", migrate_shard, destination_node);
            }

            // Spread the shards across the nodes
        }
        Command::TenantDescribe { tenant_id } => {
            let describe_response = storcon_client
                .dispatch::<(), TenantDescribeResponse>(
                    Method::GET,
                    format!("control/v1/tenant/{tenant_id}"),
                    None,
                )
                .await?;
            let shards = describe_response.shards;
            let mut table = comfy_table::Table::new();
            table.set_header(["Shard", "Attached", "Secondary", "Last error", "status"]);
            for shard in shards {
                let secondary = shard
                    .node_secondary
                    .iter()
                    .map(|n| format!("{}", n))
                    .collect::<Vec<_>>()
                    .join(",");

                let mut status_parts = Vec::new();
                if shard.is_reconciling {
                    status_parts.push("reconciling");
                }

                if shard.is_pending_compute_notification {
                    status_parts.push("pending_compute");
                }

                if shard.is_splitting {
                    status_parts.push("splitting");
                }
                let status = status_parts.join(",");

                table.add_row([
                    format!("{}", shard.tenant_shard_id),
                    shard
                        .node_attached
                        .map(|n| format!("{}", n))
                        .unwrap_or(String::new()),
                    secondary,
                    shard.last_error,
                    status,
                ]);
            }
            println!("{table}");
        }
    }

    Ok(())
}
