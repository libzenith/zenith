pub mod checks;
pub mod cloud_admin_api;
mod copied_definitions;
pub mod delete_batch_producer;
mod s3_deletion;

use std::env;
use std::fmt::Display;
use std::time::Duration;

use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::sso::SsoCredentialsProvider;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::{Client, Config};

pub use copied_definitions::id::TenantId;
use copied_definitions::id::TenantTimelineId;
pub use s3_deletion::S3Deleter;
use tracing::error;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

const MAX_RETRIES: usize = 20;
const CLOUD_ADMIN_API_TOKEN_ENV_VAR: &str = "CLOUD_ADMIN_API_TOKEN";

#[derive(Debug, Clone)]
pub struct S3Target {
    pub bucket_name: String,
    pub prefix_in_bucket: String,
    pub delimiter: String,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraversingDepth {
    Tenant,
    Timeline,
}

impl Display for TraversingDepth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Tenant => "tenant",
            Self::Timeline => "timeline",
        })
    }
}

impl S3Target {
    pub fn with_sub_segment(&self, new_segment: &str) -> Self {
        let mut new_self = self.clone();
        let _ = new_self.prefix_in_bucket.pop();
        new_self.prefix_in_bucket =
            [&new_self.prefix_in_bucket, new_segment, ""].join(&new_self.delimiter);
        new_self
    }
}

#[derive(Clone)]
pub enum RootTarget {
    Pageserver(S3Target),
    Safekeeper(S3Target),
}

impl RootTarget {
    pub fn tenants_root(&self) -> &S3Target {
        match self {
            Self::Pageserver(root) => root,
            Self::Safekeeper(root) => root,
        }
    }

    pub fn tenant_root(&self, tenant_id: TenantId) -> S3Target {
        self.tenants_root().with_sub_segment(&tenant_id.to_string())
    }

    pub fn timelines_root(&self, tenant_id: TenantId) -> S3Target {
        match self {
            Self::Pageserver(_) => self.tenant_root(tenant_id).with_sub_segment("timelines"),
            Self::Safekeeper(_) => self.tenant_root(tenant_id),
        }
    }

    pub fn timeline_root(&self, id: TenantTimelineId) -> S3Target {
        self.timelines_root(id.tenant_id)
            .with_sub_segment(&id.timeline_id.to_string())
    }

    pub fn bucket_name(&self) -> &str {
        match self {
            Self::Pageserver(root) => &root.bucket_name,
            Self::Safekeeper(root) => &root.bucket_name,
        }
    }

    pub fn delimiter(&self) -> &str {
        match self {
            Self::Pageserver(root) => &root.delimiter,
            Self::Safekeeper(root) => &root.delimiter,
        }
    }
}

pub fn get_cloud_admin_api_token_or_exit() -> String {
    match env::var(CLOUD_ADMIN_API_TOKEN_ENV_VAR) {
        Ok(token) => token,
        Err(env::VarError::NotPresent) => {
            error!("{CLOUD_ADMIN_API_TOKEN_ENV_VAR} env variable is not present");
            std::process::exit(1);
        }
        Err(env::VarError::NotUnicode(not_unicode_string)) => {
            error!("{CLOUD_ADMIN_API_TOKEN_ENV_VAR} env variable's value is not a valid unicode string: {not_unicode_string:?}");
            std::process::exit(1);
        }
    }
}

pub fn init_logging(binary_name: &str, dry_run: bool, node_kind: &str) -> WorkerGuard {
    let file_name = if dry_run {
        format!(
            "{}_{}_{}__dry.log",
            binary_name,
            node_kind,
            chrono::Utc::now().format("%Y_%m_%d__%H_%M_%S")
        )
    } else {
        format!(
            "{}_{}_{}.log",
            binary_name,
            node_kind,
            chrono::Utc::now().format("%Y_%m_%d__%H_%M_%S")
        )
    };

    let (file_writer, guard) =
        tracing_appender::non_blocking(tracing_appender::rolling::never("./logs/", file_name));

    let file_logs = fmt::Layer::new()
        .with_target(false)
        .with_ansi(false)
        .with_writer(file_writer);
    let stdout_logs = fmt::Layer::new()
        .with_target(false)
        .with_ansi(atty::is(atty::Stream::Stdout))
        .with_writer(std::io::stdout);
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(file_logs)
        .with(stdout_logs)
        .init();

    guard
}

pub fn init_s3_client(account_id: String, bucket_region: Region) -> Client {
    let credentials_provider = {
        // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
        CredentialsProviderChain::first_try("env", EnvironmentVariableCredentialsProvider::new())
            // uses sso
            .or_else(
                "sso",
                SsoCredentialsProvider::builder()
                    .account_id(account_id)
                    .role_name("PowerUserAccess")
                    .start_url("https://neondb.awsapps.com/start")
                    .region(Region::from_static("eu-central-1"))
                    .build(),
            )
            // uses imds v2
            .or_else("imds", ImdsCredentialsProvider::builder().build())
    };

    let config = Config::builder()
        .region(bucket_region)
        .credentials_provider(credentials_provider)
        .build();

    Client::from_conf(config)
}

async fn list_objects_with_retries(
    s3_client: &Client,
    s3_target: &S3Target,
    continuation_token: Option<String>,
) -> anyhow::Result<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output> {
    for _ in 0..MAX_RETRIES {
        match s3_client
            .list_objects_v2()
            .bucket(&s3_target.bucket_name)
            .prefix(&s3_target.prefix_in_bucket)
            .delimiter(&s3_target.delimiter)
            .set_continuation_token(continuation_token.clone())
            .send()
            .await
        {
            Ok(response) => return Ok(response),
            Err(e) => {
                error!("list_objects_v2 query failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    anyhow::bail!("Failed to list objects {MAX_RETRIES} times")
}
