//! A private CA and a rustls server for rtmps tests.

use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::PrivateKeyDer;

pub struct Pki {
    /// The CA certificate (DER), to be trusted through the test-only hook.
    pub ca_der: Vec<u8>,
    pub acceptor: TlsAcceptor,
}

/// A CA and a server certificate for `names`, signed by it.
pub fn pki(names: &[&str]) -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "fs-streaming test CA");
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);
    let key = KeyPair::generate().unwrap();
    let cert = CertificateParams::new(names.iter().map(|n| (*n).to_string()).collect::<Vec<_>>())
        .unwrap()
        .signed_by(&key, &issuer)
        .unwrap();
    let config = ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            PrivateKeyDer::try_from(key.serialize_der()).unwrap(),
        )
        .unwrap();
    Pki {
        ca_der: ca.der().to_vec(),
        acceptor: TlsAcceptor::from(Arc::new(config)),
    }
}
