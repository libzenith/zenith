use std::env;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context};
use rustls::crypto::ring;

/// We use an internal certificate authority when establishing a TLS connection with compute.
fn load_internal_certs(store: &mut rustls::RootCertStore) -> anyhow::Result<()> {
    let Some(ca_file) = env::var_os("NEON_INTERNAL_CA_FILE") else {
        return Ok(());
    };
    let ca_file = PathBuf::from(ca_file);

    let ca = std::fs::read(&ca_file)
        .with_context(|| format!("could not read CA from {}", ca_file.display()))?;

    for cert in rustls_pemfile::certs(&mut Cursor::new(&*ca)) {
        store
            .add(cert.context("could not parse internal CA certificate")?)
            .context("could not parse internal CA certificate")?;
    }

    Ok(())
}

/// For console redirect proxy, we need to establish a connection to compute via pg-sni-router.
/// pg-sni-router needs TLS and uses a Let's Encrypt signed certificate, so we
/// load certificates from our native store.
fn load_native_certs(store: &mut rustls::RootCertStore) -> anyhow::Result<()> {
    let der_certs = rustls_native_certs::load_native_certs();

    if !der_certs.errors.is_empty() {
        bail!("could not parse certificates: {:?}", der_certs.errors);
    }

    store.add_parsable_certificates(der_certs.certs);

    Ok(())
}

fn load_compute_certs() -> anyhow::Result<Arc<rustls::RootCertStore>> {
    let mut store = rustls::RootCertStore::empty();
    load_native_certs(&mut store)?;
    load_internal_certs(&mut store)?;
    Ok(Arc::new(store))
}

/// Loads the root certificates and constructs a client config suitable for connecting to the neon compute.
/// This function is blocking.
pub fn compute_client_config_with_root_certs() -> anyhow::Result<rustls::ClientConfig> {
    Ok(
        rustls::ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring should support the default protocol versions")
            .with_root_certificates(load_compute_certs()?)
            .with_no_client_auth(),
    )
}

#[cfg(test)]
pub fn compute_client_config_with_certs(
    certs: impl IntoIterator<Item = rustls::pki_types::CertificateDer<'static>>,
) -> rustls::ClientConfig {
    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(certs);

    rustls::ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("ring should support the default protocol versions")
        .with_root_certificates(store)
        .with_no_client_auth()
}
