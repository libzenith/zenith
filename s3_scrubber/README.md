# Neon S3 scrubber

This tool directly accesses the S3 buckets used by the Neon `pageserver`
and `safekeeper`, and does housekeeping such as cleaning up objects for tenants & timelines that no longer exist.

## Usage

### Generic Parameters

#### S3

Do `aws sso login --profile dev` to get the SSO access to the bucket to clean, get the SSO_ACCOUNT_ID for your profile (`cat ~/.aws/config` may help).

- `SSO_ACCOUNT_ID`: Credentials id to use for accessing S3 buckets
- `REGION`: A region where the bucket is located at.
- `BUCKET`: Bucket name

#### Console API

- `CLOUD_ADMIN_API_URL`: The URL base to use for checking tenant/timeline for existence via the Cloud API.

- `CLOUD_ADMIN_API_TOKEN`: The token to provide when querying the admin API. Get one on the corresponding console page, e.g. https://console.stage.neon.tech/app/settings/api-keys

### Commands

#### `tidy`

Iterate over S3 buckets for storage nodes, checking their contents and removing the data not present in the console. Node S3 data that's not removed is then further checked for discrepancies and, sometimes, validated.

Unless the global `--delete` argument is provided, this command only dry-runs and logs
what it would have deleted.

```
tidy --node-kind=<safekeeper|pageserver> [--depth=<tenant|timeline>] [--skip-validation]
```

- `--node-kind`: whether to inspect safekeeper or pageserver bucket prefix
- `--depth`: whether to only search for deletable tenants, or also search for
  deletable timelines within active tenants. Default: `tenant`
- `--skip-validation`: skip additional post-deletion checks. Default: `false`

For a selected S3 path, the tool lists the S3 bucket given for either tenants or both tenants and timelines — for every found entry, console API is queried: any deleted or missing in the API entity is scheduled for deletion from S3.

If validation is enabled, only the non-deleted tenants' ones are checked.
For pageserver, timelines' index_part.json on S3 is also checked for various discrepancies: no files are removed, even if there are "extra" S3 files not present in index_part.json: due to the way pageserver updates the remote storage, it's better to do such removals manually, stopping the corresponding tenant first.

Command examples:

`env SSO_ACCOUNT_ID=369495373322 REGION=eu-west-1 BUCKET=neon-dev-storage-eu-west-1 CLOUD_ADMIN_API_TOKEN=${NEON_CLOUD_ADMIN_API_STAGING_KEY} CLOUD_ADMIN_API_URL=https://console.stage.neon.tech/admin cargo run --release -- tidy --node-kind=safekeeper`

`env SSO_ACCOUNT_ID=369495373322 REGION=us-east-2 BUCKET=neon-staging-storage-us-east-2 CLOUD_ADMIN_API_TOKEN=${NEON_CLOUD_ADMIN_API_STAGING_KEY} CLOUD_ADMIN_API_URL=https://console.stage.neon.tech/admin cargo run --release -- tidy --node-kind=pageserver --depth=timeline`

When dry run stats look satisfying, use `-- --delete` before the `tidy` command to
disable dry run and run the binary with deletion enabled.

See these lines (and lines around) in the logs for the final stats:

- `Finished listing the bucket for tenants`
- `Finished active tenant and timeline validation`
- `Total tenant deletion stats`
- `Total timeline deletion stats`

## Current implementation details

- The tool does not have any peristent state currently: instead, it creates very verbose logs, with every S3 delete request logged, every tenant/timeline id check, etc.
  Worse, any panic or early errored tasks might force the tool to exit without printing the final summary — all affected ids will still be in the logs though. The tool has retries inside it, so it's error-resistant up to some extent, and recent runs showed no traces of errors/panics.

- Instead of checking non-deleted tenants' timelines instantly, the tool attempts to create separate tasks (futures) for that,
  complicating the logic and slowing down the process, this should be fixed and done in one "task".

- The tool does uses only publicly available remote resources (S3, console) and does not access pageserver/safekeeper nodes themselves.
  Yet, its S3 set up should be prepared for running on any pageserver/safekeeper node, using node's S3 credentials, so the node API access logic could be implemented relatively simply on top.

- Tool has `copied_definitions` module that contains copy-pasted storage primitives with parsing and deserializing definitions for them — that might get out of date with future storage development and should be solved either by moving the storage definitions into a separate crate (then it can be referenced via git url already) or unifying the clenup project with the storage (unclear value: extra infrastructure metadata exposure + garbage tenants and timelines in storage is a bug and should not appear normally, the work on this is ongoing: https://github.com/neondatabase/neon/issues/3889)
  Similarly S3 paths for safekeeper and pageserver are hardcoded, but not likely the structure changes in the near future.

### Future concerns

If the tool is left as is, a number of things might break:

- `copied_definitions` might get out of date and become incompatible with the future format changes: do update these regularly or library-ify/merge the codebases

- Console Admin API seems to be using "v1" version? V2 is already in the wild and the old version might be obsoleted eventually

- S3 format will change for paths also or the layer creation/deletion logic might change on pageserver: if tool does not get obsolete, it needs to get adapted for this

## Cleanup procedure:

### Pageserver preparations

If S3 state is altered first manually, pageserver in-memory state will contain wrong data about S3 state, and tenants/timelines may get recreated on S3 (due to any layer upload due to compaction, pageserver restart, etc.). So before proceeding, for tenants/timelines which are already deleted in the console, we must remove these from pageservers.

First, we need to group pageservers by buckets, https://console.stage.neon.tech/admin/pageservers can be used for all env nodes, then `cat /storage/pageserver/data/pageserver.toml` on every node will show the bucket names and regions needed.

Per bucket, for every pageserver id related, find deleted tenants:

`curl -X POST "https://console.stage.neon.tech/admin/check_pageserver/{id}" -H "Accept: application/json" -H "Authorization: Bearer ${NEON_CLOUD_ADMIN_API_STAGING_KEY}" | jq`

use `?check_timelines=true` to find deleted timelines, but the check runs a separate query on every alive tenant, so that could be long and time out for big pageservers.

Note that some tenants/timelines could be marked as deleted in console, but console might continue querying the node later to fully remove the tenant/timeline: wait for some time before ensuring that the "extra" tenant/timeline is not going away by itself.

When all IDs are collected, manually go to every pageserver and detach/delete the tenant/timeline.
In future, the cleanup tool may access pageservers directly, but now it's only console and S3 it has access to.
