use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
use futures::future::Either;
use proxy::auth;
use proxy::auth::backend::MaybeOwned;
use proxy::cancellation::CancelMap;
use proxy::cancellation::CancellationHandler;
use proxy::config::remote_storage_from_toml;
use proxy::config::AuthenticationConfig;
use proxy::config::CacheOptions;
use proxy::config::HttpConfig;
use proxy::config::ProjectInfoCacheOptions;
use proxy::console;
use proxy::context::parquet::ParquetUploadArgs;
use proxy::http;
use proxy::metrics::NUM_CANCELLATION_REQUESTS_SOURCE_FROM_CLIENT;
use proxy::rate_limiter::AuthRateLimiter;
use proxy::rate_limiter::EndpointRateLimiter;
use proxy::rate_limiter::RateBucketInfo;
use proxy::rate_limiter::RateLimiterConfig;
use proxy::redis::cancellation_publisher::RedisPublisherClient;
use proxy::redis::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use proxy::redis::elasticache;
use proxy::redis::notifications;
use proxy::serverless::GlobalConnPoolOptions;
use proxy::usage_metrics;

use anyhow::bail;
use proxy::config::{self, ProxyConfig};
use proxy::serverless;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;
use utils::{project_build_tag, project_git_version, sentry_init::init_sentry};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

use clap::{Parser, ValueEnum};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone, Debug, ValueEnum)]
enum AuthBackend {
    Console,
    #[cfg(feature = "testing")]
    Postgres,
    Link,
}

/// Neon proxy/router
#[derive(Parser)]
#[command(version = GIT_VERSION, about)]
struct ProxyCliArgs {
    /// Name of the region this proxy is deployed in
    #[clap(long, default_value_t = String::new())]
    region: String,
    /// listen for incoming client connections on ip:port
    #[clap(short, long, default_value = "127.0.0.1:4432")]
    proxy: String,
    #[clap(value_enum, long, default_value_t = AuthBackend::Link)]
    auth_backend: AuthBackend,
    /// listen for management callback connection on ip:port
    #[clap(short, long, default_value = "127.0.0.1:7000")]
    mgmt: String,
    /// listen for incoming http connections (metrics, etc) on ip:port
    #[clap(long, default_value = "127.0.0.1:7001")]
    http: String,
    /// listen for incoming wss connections on ip:port
    #[clap(long)]
    wss: Option<String>,
    /// redirect unauthenticated users to the given uri in case of link auth
    #[clap(short, long, default_value = "http://localhost:3000/psql_session/")]
    uri: String,
    /// cloud API endpoint for authenticating users
    #[clap(
        short,
        long,
        default_value = "http://localhost:3000/authenticate_proxy_request/"
    )]
    auth_endpoint: String,
    /// path to TLS key for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'k', long, alias = "ssl-key")]
    tls_key: Option<String>,
    /// path to TLS cert for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'c', long, alias = "ssl-cert")]
    tls_cert: Option<String>,
    /// path to directory with TLS certificates for client postgres connections
    #[clap(long)]
    certs_dir: Option<String>,
    /// timeout for the TLS handshake
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    handshake_timeout: tokio::time::Duration,
    /// http endpoint to receive periodic metric updates
    #[clap(long)]
    metric_collection_endpoint: Option<String>,
    /// how often metrics should be sent to a collection endpoint
    #[clap(long)]
    metric_collection_interval: Option<String>,
    /// cache for `wake_compute` api method (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    wake_compute_cache: String,
    /// lock for `wake_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::WakeComputeLockOptions::DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK)]
    wake_compute_lock: String,
    /// Allow self-signed certificates for compute nodes (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    allow_self_signed_compute: bool,
    #[clap(flatten)]
    sql_over_http: SqlOverHttpArgs,
    /// timeout for scram authentication protocol
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    scram_protocol_timeout: tokio::time::Duration,
    /// Require that all incoming requests have a Proxy Protocol V2 packet **and** have an IP address associated.
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    require_client_ip: bool,
    /// Disable dynamic rate limiter and store the metrics to ensure its production behaviour.
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    disable_dynamic_rate_limiter: bool,
    /// Rate limit algorithm. Makes sense only if `disable_rate_limiter` is `false`.
    #[clap(value_enum, long, default_value_t = proxy::rate_limiter::RateLimitAlgorithm::Aimd)]
    rate_limit_algorithm: proxy::rate_limiter::RateLimitAlgorithm,
    /// Timeout for rate limiter. If it didn't manage to aquire a permit in this time, it will return an error.
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    rate_limiter_timeout: tokio::time::Duration,
    /// Endpoint rate limiter max number of requests per second.
    ///
    /// Provided in the form '<Requests Per Second>@<Bucket Duration Size>'.
    /// Can be given multiple times for different bucket sizes.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_ENDPOINT_SET)]
    endpoint_rps_limit: Vec<RateBucketInfo>,
    /// Whether the auth rate limiter actually takes effect (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    auth_rate_limit_enabled: bool,
    /// Authentication rate limiter max number of hashes per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_AUTH_SET)]
    auth_rate_limit: Vec<RateBucketInfo>,
    /// Redis rate limiter max number of requests per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_ENDPOINT_SET)]
    redis_rps_limit: Vec<RateBucketInfo>,
    /// Initial limit for dynamic rate limiter. Makes sense only if `rate_limit_algorithm` is *not* `None`.
    #[clap(long, default_value_t = 100)]
    initial_limit: usize,
    #[clap(flatten)]
    aimd_config: proxy::rate_limiter::AimdConfig,
    /// cache for `allowed_ips` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    allowed_ips_cache: String,
    /// cache for `role_secret` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    role_secret_cache: String,
    /// disable ip check for http requests. If it is too time consuming, it could be turned off.
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    disable_ip_check_for_http: bool,
    /// redis url for notifications (if empty, redis_host:port will be used for both notifications and streaming connections)
    #[clap(long)]
    redis_notifications: Option<String>,
    /// redis host for streaming connections (might be different from the notifications host)
    #[clap(long)]
    redis_host: Option<String>,
    /// redis port for streaming connections (might be different from the notifications host)
    #[clap(long)]
    redis_port: Option<u16>,
    /// redis cluster name, used in aws elasticache
    #[clap(long)]
    redis_cluster_name: Option<String>,
    /// redis user_id, used in aws elasticache
    #[clap(long)]
    redis_user_id: Option<String>,
    /// aws region to retrieve credentials
    #[clap(long, default_value_t = String::new())]
    aws_region: String,
    /// cache for `project_info` (use `size=0` to disable)
    #[clap(long, default_value = config::ProjectInfoCacheOptions::CACHE_DEFAULT_OPTIONS)]
    project_info_cache: String,

    #[clap(flatten)]
    parquet_upload: ParquetUploadArgs,

    /// interval for backup metric collection
    #[clap(long, default_value = "10m", value_parser = humantime::parse_duration)]
    metric_backup_collection_interval: std::time::Duration,
    /// remote storage configuration for backup metric collection
    /// Encoded as toml (same format as pageservers), eg
    /// `{bucket_name='the-bucket',bucket_region='us-east-1',prefix_in_bucket='proxy',endpoint='http://minio:9000'}`
    #[clap(long, default_value = "{}")]
    metric_backup_collection_remote_storage: String,
    /// chunk size for backup metric collection
    /// Size of each event is no more than 400 bytes, so 2**22 is about 200MB before the compression.
    #[clap(long, default_value = "4194304")]
    metric_backup_collection_chunk_size: usize,
}

#[derive(clap::Args, Clone, Copy, Debug)]
struct SqlOverHttpArgs {
    /// timeout for http connection requests
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    sql_over_http_timeout: tokio::time::Duration,

    /// Whether the SQL over http pool is opt-in
    #[clap(long, default_value_t = true, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    sql_over_http_pool_opt_in: bool,

    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 20)]
    sql_over_http_pool_max_conns_per_endpoint: usize,

    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 20000)]
    sql_over_http_pool_max_total_conns: usize,

    /// How long pooled connections should remain idle for before closing
    #[clap(long, default_value = "5m", value_parser = humantime::parse_duration)]
    sql_over_http_idle_timeout: tokio::time::Duration,

    /// Duration each shard will wait on average before a GC sweep.
    /// A longer time will causes sweeps to take longer but will interfere less frequently.
    #[clap(long, default_value = "10m", value_parser = humantime::parse_duration)]
    sql_over_http_pool_gc_epoch: tokio::time::Duration,

    /// How many shards should the global pool have. Must be a power of two.
    /// More shards will introduce less contention for pool operations, but can
    /// increase memory used by the pool
    #[clap(long, default_value_t = 128)]
    sql_over_http_pool_shards: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    info!("Version: {GIT_VERSION}");
    info!("Build_tag: {BUILD_TAG}");
    ::metrics::set_build_info_metric(GIT_VERSION, BUILD_TAG);

    match proxy::jemalloc::MetricRecorder::new(prometheus::default_registry()) {
        Ok(t) => {
            t.start();
        }
        Err(e) => tracing::error!(error = ?e, "could not start jemalloc metrics loop"),
    }

    let args = ProxyCliArgs::parse();
    let config = build_config(&args)?;

    info!("Authentication backend: {}", config.auth_backend);
    info!("Using region: {}", config.aws_region);

    let region_provider = RegionProviderChain::default_provider().or_else(&*config.aws_region); // Replace with your Redis region if needed
    let provider_conf =
        ProviderConfig::without_region().with_region(region_provider.region().await);
    let aws_credentials_provider = {
        // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
        CredentialsProviderChain::first_try("env", EnvironmentVariableCredentialsProvider::new())
            // uses "AWS_PROFILE" / `aws sso login --profile <profile>`
            .or_else(
                "profile-sso",
                ProfileFileCredentialsProvider::builder()
                    .configure(&provider_conf)
                    .build(),
            )
            // uses "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME"
            // needed to access remote extensions bucket
            .or_else(
                "token",
                WebIdentityTokenCredentialsProvider::builder()
                    .configure(&provider_conf)
                    .build(),
            )
            // uses imds v2
            .or_else("imds", ImdsCredentialsProvider::builder().build())
    };
    let elasticache_credentials_provider = Arc::new(elasticache::CredentialsProvider::new(
        elasticache::AWSIRSAConfig::new(
            config.aws_region.clone(),
            args.redis_cluster_name,
            args.redis_user_id,
        ),
        aws_credentials_provider,
    ));
    let redis_notifications_client =
        match (args.redis_notifications, (args.redis_host, args.redis_port)) {
            (Some(url), _) => {
                info!("Starting redis notifications listener ({url})");
                Some(ConnectionWithCredentialsProvider::new_with_static_credentials(url))
            }
            (None, (Some(host), Some(port))) => Some(
                ConnectionWithCredentialsProvider::new_with_credentials_provider(
                    host,
                    port,
                    elasticache_credentials_provider.clone(),
                ),
            ),
            (None, (None, None)) => {
                warn!("Redis is disabled");
                None
            }
            _ => {
                bail!("redis-host and redis-port must be specified together");
            }
        };

    // Check that we can bind to address before further initialization
    let http_address: SocketAddr = args.http.parse()?;
    info!("Starting http on {http_address}");
    let http_listener = TcpListener::bind(http_address).await?.into_std()?;

    let mgmt_address: SocketAddr = args.mgmt.parse()?;
    info!("Starting mgmt on {mgmt_address}");
    let mgmt_listener = TcpListener::bind(mgmt_address).await?;

    let proxy_address: SocketAddr = args.proxy.parse()?;
    info!("Starting proxy on {proxy_address}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;
    let cancellation_token = CancellationToken::new();

    let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new(&config.endpoint_rps_limit));
    let cancel_map = CancelMap::default();

    // let redis_notifications_client = redis_notifications_client.map(|x| Box::leak(Box::new(x)));
    let redis_publisher = match &redis_notifications_client {
        Some(redis_publisher) => Some(Arc::new(Mutex::new(RedisPublisherClient::new(
            redis_publisher.clone(),
            args.region.clone(),
            &config.redis_rps_limit,
        )?))),
        None => None,
    };
    let cancellation_handler = Arc::new(CancellationHandler::<
        Option<Arc<tokio::sync::Mutex<RedisPublisherClient>>>,
    >::new(
        cancel_map.clone(),
        redis_publisher,
        NUM_CANCELLATION_REQUESTS_SOURCE_FROM_CLIENT,
    ));

    // client facing tasks. these will exit on error or on cancellation
    // cancellation returns Ok(())
    let mut client_tasks = JoinSet::new();
    client_tasks.spawn(proxy::proxy::task_main(
        config,
        proxy_listener,
        cancellation_token.clone(),
        endpoint_rate_limiter.clone(),
        cancellation_handler.clone(),
    ));

    // TODO: rename the argument to something like serverless.
    // It now covers more than just websockets, it also covers SQL over HTTP.
    if let Some(serverless_address) = args.wss {
        let serverless_address: SocketAddr = serverless_address.parse()?;
        info!("Starting wss on {serverless_address}");
        let serverless_listener = TcpListener::bind(serverless_address).await?;

        client_tasks.spawn(serverless::task_main(
            config,
            serverless_listener,
            cancellation_token.clone(),
            endpoint_rate_limiter.clone(),
            cancellation_handler.clone(),
        ));
    }

    client_tasks.spawn(proxy::context::parquet::worker(
        cancellation_token.clone(),
        args.parquet_upload,
    ));

    // maintenance tasks. these never return unless there's an error
    let mut maintenance_tasks = JoinSet::new();
    maintenance_tasks.spawn(proxy::handle_signals(cancellation_token.clone()));
    maintenance_tasks.spawn(http::health_server::task_main(http_listener));
    maintenance_tasks.spawn(console::mgmt::task_main(mgmt_listener));

    if let Some(metrics_config) = &config.metric_collection {
        // TODO: Add gc regardles of the metric collection being enabled.
        maintenance_tasks.spawn(usage_metrics::task_main(metrics_config));
        client_tasks.spawn(usage_metrics::task_backup(
            &metrics_config.backup_metric_collection_config,
            cancellation_token,
        ));
    }

    if let auth::BackendType::Console(api, _) = &config.auth_backend {
        if let proxy::console::provider::ConsoleBackend::Console(api) = &**api {
            if let Some(redis_notifications_client) = redis_notifications_client {
                let cache = api.caches.project_info.clone();
                maintenance_tasks.spawn(notifications::task_main(
                    redis_notifications_client.clone(),
                    cache.clone(),
                    cancel_map.clone(),
                    args.region.clone(),
                ));
                maintenance_tasks.spawn(async move { cache.clone().gc_worker().await });
            }
        }
    }

    let maintenance = loop {
        // get one complete task
        match futures::future::select(
            pin!(maintenance_tasks.join_next()),
            pin!(client_tasks.join_next()),
        )
        .await
        {
            // exit immediately on maintenance task completion
            Either::Left((Some(res), _)) => break proxy::flatten_err(res)?,
            // exit with error immediately if all maintenance tasks have ceased (should be caught by branch above)
            Either::Left((None, _)) => bail!("no maintenance tasks running. invalid state"),
            // exit immediately on client task error
            Either::Right((Some(res), _)) => proxy::flatten_err(res)?,
            // exit if all our client tasks have shutdown gracefully
            Either::Right((None, _)) => return Ok(()),
        }
    };

    // maintenance tasks return Infallible success values, this is an impossible value
    // so this match statically ensures that there are no possibilities for that value
    match maintenance {}
}

/// ProxyConfig is created at proxy startup, and lives forever.
fn build_config(args: &ProxyCliArgs) -> anyhow::Result<&'static ProxyConfig> {
    let tls_config = match (&args.tls_key, &args.tls_cert) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_tls(
            key_path,
            cert_path,
            args.certs_dir.as_ref(),
        )?),
        (None, None) => None,
        _ => bail!("either both or neither tls-key and tls-cert must be specified"),
    };

    if args.allow_self_signed_compute {
        warn!("allowing self-signed compute certificates");
    }
    let backup_metric_collection_config = config::MetricBackupCollectionConfig {
        interval: args.metric_backup_collection_interval,
        remote_storage_config: remote_storage_from_toml(
            &args.metric_backup_collection_remote_storage,
        )?,
        chunk_size: args.metric_backup_collection_chunk_size,
    };

    let metric_collection = match (
        &args.metric_collection_endpoint,
        &args.metric_collection_interval,
    ) {
        (Some(endpoint), Some(interval)) => Some(config::MetricCollectionConfig {
            endpoint: endpoint.parse()?,
            interval: humantime::parse_duration(interval)?,
            backup_metric_collection_config,
        }),
        (None, None) => None,
        _ => bail!(
            "either both or neither metric-collection-endpoint \
             and metric-collection-interval must be specified"
        ),
    };
    let rate_limiter_config = RateLimiterConfig {
        disable: args.disable_dynamic_rate_limiter,
        algorithm: args.rate_limit_algorithm,
        timeout: args.rate_limiter_timeout,
        initial_limit: args.initial_limit,
        aimd_config: Some(args.aimd_config),
    };

    let auth_backend = match &args.auth_backend {
        AuthBackend::Console => {
            let wake_compute_cache_config: CacheOptions = args.wake_compute_cache.parse()?;
            let project_info_cache_config: ProjectInfoCacheOptions =
                args.project_info_cache.parse()?;

            info!("Using NodeInfoCache (wake_compute) with options={wake_compute_cache_config:?}");
            info!(
                "Using AllowedIpsCache (wake_compute) with options={project_info_cache_config:?}"
            );
            let caches = Box::leak(Box::new(console::caches::ApiCaches::new(
                wake_compute_cache_config,
                project_info_cache_config,
            )));

            let config::WakeComputeLockOptions {
                shards,
                permits,
                epoch,
                timeout,
            } = args.wake_compute_lock.parse()?;
            info!(permits, shards, ?epoch, "Using NodeLocks (wake_compute)");
            let locks = Box::leak(Box::new(
                console::locks::ApiLocks::new("wake_compute_lock", permits, shards, timeout)
                    .unwrap(),
            ));
            tokio::spawn(locks.garbage_collect_worker(epoch));

            let url = args.auth_endpoint.parse()?;
            let endpoint = http::Endpoint::new(url, http::new_client(rate_limiter_config));

            let api = console::provider::neon::Api::new(endpoint, caches, locks);
            let api = console::provider::ConsoleBackend::Console(api);
            auth::BackendType::Console(MaybeOwned::Owned(api), ())
        }
        #[cfg(feature = "testing")]
        AuthBackend::Postgres => {
            let url = args.auth_endpoint.parse()?;
            let api = console::provider::mock::Api::new(url);
            let api = console::provider::ConsoleBackend::Postgres(api);
            auth::BackendType::Console(MaybeOwned::Owned(api), ())
        }
        AuthBackend::Link => {
            let url = args.uri.parse()?;
            auth::BackendType::Link(MaybeOwned::Owned(url), ())
        }
    };
    let http_config = HttpConfig {
        request_timeout: args.sql_over_http.sql_over_http_timeout,
        pool_options: GlobalConnPoolOptions {
            max_conns_per_endpoint: args.sql_over_http.sql_over_http_pool_max_conns_per_endpoint,
            gc_epoch: args.sql_over_http.sql_over_http_pool_gc_epoch,
            pool_shards: args.sql_over_http.sql_over_http_pool_shards,
            idle_timeout: args.sql_over_http.sql_over_http_idle_timeout,
            opt_in: args.sql_over_http.sql_over_http_pool_opt_in,
            max_total_conns: args.sql_over_http.sql_over_http_pool_max_total_conns,
        },
    };
    let authentication_config = AuthenticationConfig {
        scram_protocol_timeout: args.scram_protocol_timeout,
        rate_limiter_enabled: args.auth_rate_limit_enabled,
        rate_limiter: AuthRateLimiter::new(args.auth_rate_limit.clone()),
    };

    let mut endpoint_rps_limit = args.endpoint_rps_limit.clone();
    RateBucketInfo::validate(&mut endpoint_rps_limit)?;
    let mut redis_rps_limit = args.redis_rps_limit.clone();
    RateBucketInfo::validate(&mut redis_rps_limit)?;

    let config = Box::leak(Box::new(ProxyConfig {
        tls_config,
        auth_backend,
        metric_collection,
        allow_self_signed_compute: args.allow_self_signed_compute,
        http_config,
        authentication_config,
        require_client_ip: args.require_client_ip,
        disable_ip_check_for_http: args.disable_ip_check_for_http,
        endpoint_rps_limit,
        redis_rps_limit,
        handshake_timeout: args.handshake_timeout,
        region: args.region.clone(),
        aws_region: args.aws_region.clone(),
    }));

    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;
    use proxy::rate_limiter::RateBucketInfo;

    #[test]
    fn parse_endpoint_rps_limit() {
        let config = super::ProxyCliArgs::parse_from([
            "proxy",
            "--endpoint-rps-limit",
            "100@1s",
            "--endpoint-rps-limit",
            "20@30s",
        ]);

        assert_eq!(
            config.endpoint_rps_limit,
            vec![
                RateBucketInfo::new(100, Duration::from_secs(1)),
                RateBucketInfo::new(20, Duration::from_secs(30)),
            ]
        );
    }
}
