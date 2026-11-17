# Vendored dependency

This is the published `mysql-binlog-connector-rust` 0.3.5 crate from [apecloud/mysql-binlog-connector-rust](https://github.com/apecloud/mysql-binlog-connector-rust), distributed under the MIT or Apache-2.0 license (copies are next to this file).

Binflux carries a small local patch because the upstream rustls code used a custom verifier that accepted every server certificate. The Binflux version validates the server hostname and chains against the WebPKI roots or a PEM CA supplied as `ssl-ca` in the connection URL. The corresponding config field is `source.tls_ca_cert`. It also removes an unused integration-test target from the vendored manifest and includes SQL error code/state in connector errors.

Review dependency updates against the upstream release before replacing this directory. The lockfile pins all resolved transitive versions.
