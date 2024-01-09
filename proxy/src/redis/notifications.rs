use std::{convert::Infallible, sync::Arc};

use futures::StreamExt;
use redis::aio::PubSub;
use serde::Deserialize;
use smol_str::SmolStr;

use crate::cache::project_info::ProjectInfoCache;

const CHANNEL_NAME: &str = "neondb-proxy-ws-updates";

struct ConsoleRedisClient {
    client: redis::Client,
}

impl ConsoleRedisClient {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self { client })
    }
    async fn try_connect(&self) -> anyhow::Result<PubSub> {
        let mut conn = self.client.get_async_connection().await?.into_pubsub();
        tracing::info!("subscribing to a channel `{CHANNEL_NAME}`");
        conn.subscribe(CHANNEL_NAME).await?;
        Ok(conn)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "topic", content = "data")]
enum Notification {
    #[serde(
        rename = "/allowed_ips_updated",
        deserialize_with = "deserialize_allowed_ips"
    )]
    AllowedIpsUpdate { project_id: SmolStr },
    #[serde(
        rename = "/password_updated",
        deserialize_with = "deserialize_password_updated"
    )]
    PasswordUpdate {
        project_id: SmolStr,
        role_name: SmolStr,
    },
}

fn deserialize_allowed_ips<'de, D>(deserializer: D) -> Result<SmolStr, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let project = serde_json::from_str::<serde_json::Value>(&s)
        .map_err(serde::de::Error::custom)?
        .get("project")
        .ok_or_else(|| serde::de::Error::custom("no project field"))?
        .as_str()
        .ok_or_else(|| serde::de::Error::custom("project is not a string"))?
        .to_string();
    Ok(project.into())
}

fn deserialize_password_updated<'de, D>(deserializer: D) -> Result<(SmolStr, SmolStr), D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let data = serde_json::from_str::<serde_json::Value>(&s).map_err(serde::de::Error::custom)?;
    let project = data
        .get("project")
        .ok_or_else(|| serde::de::Error::custom("no project field"))?
        .as_str()
        .ok_or_else(|| serde::de::Error::custom("project is not a string"))?
        .to_string();
    let role = data
        .get("role")
        .ok_or_else(|| serde::de::Error::custom("no role field"))?
        .as_str()
        .ok_or_else(|| serde::de::Error::custom("role is not a string"))?
        .to_string();
    Ok((project.into(), role.into()))
}

fn invalidate_cache<C: ProjectInfoCache>(cache: Arc<C>, msg: Notification) {
    use Notification::*;
    match msg {
        AllowedIpsUpdate { project } => cache.invalidate_allowed_ips_for_project(&project),
        PasswordUpdate { project, role } => {
            cache.invalidate_role_secret_for_project(&project, &role)
        }
    }
}

#[tracing::instrument(skip(cache))]
fn handle_message<C>(msg: redis::Msg, cache: Arc<C>) -> anyhow::Result<()>
where
    C: ProjectInfoCache + Send + Sync + 'static,
{
    let payload: String = msg.get_payload()?;

    let msg: Notification = match serde_json::from_str(&payload) {
        Ok(msg) => msg,
        Err(e) => {
            tracing::error!("broken message: {e}");
            return Ok(());
        }
    };
    tracing::trace!(?msg, "received a message");
    invalidate_cache(cache.clone(), msg.clone());
    // It might happen that the invalid entry is on the way to be cached.
    // To make sure that the entry is invalidated, let's repeat the invalidation in 20 seconds.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        invalidate_cache(cache, msg.clone());
    });

    Ok(())
}

/// Handle console's invalidation messages.
#[tracing::instrument(name = "console_notifications", skip_all)]
pub async fn task_main<C>(url: String, cache: Arc<C>) -> anyhow::Result<Infallible>
where
    C: ProjectInfoCache + Send + Sync + 'static,
{
    cache.enable_ttl();

    loop {
        let redis = ConsoleRedisClient::new(&url)?;
        let conn = match redis.try_connect().await {
            Ok(conn) => {
                cache.disable_ttl();
                conn
            }
            Err(e) => {
                tracing::error!(
                    "failed to connect to redis: {e}, will try to reconnect in 100 seconds"
                );
                tokio::time::sleep(std::time::Duration::from_secs(100)).await;
                continue;
            }
        };
        let mut stream = conn.into_on_message();
        while let Some(msg) = stream.next().await {
            match handle_message(msg, cache.clone()) {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("failed to handle message: {e}, will try to reconnect");
                    break;
                }
            }
        }
        cache.enable_ttl();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_allowed_ips() -> anyhow::Result<()> {
        let project = "new_project".to_string();
        let data = format!("{{\"project\": \"{project}\"}}");
        let text = json!({
            "type": "message",
            "topic": "/allowed_ips_updated",
            "data": data,
            "extre_fields": "something"
        })
        .to_string();

        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(
            result,
            Notification::AllowedIpsUpdate {
                project: project.into()
            }
        );

        Ok(())
    }

    #[test]
    fn parse_password_updated() -> anyhow::Result<()> {
        let project = "new_project".to_string();
        let role = "new_role".to_string();
        let data = format!("{{\"project\": \"{project}\", \"role\": \"{role}\"}}");
        let text = json!({
            "type": "message",
            "topic": "/password_updated",
            "data": data,
            "extre_fields": "something"
        })
        .to_string();

        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(
            result,
            Notification::PasswordUpdate {
                project: project.into(),
                role: role.into()
            }
        );

        Ok(())
    }
}
