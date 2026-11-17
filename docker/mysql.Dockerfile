FROM mysql:8.4
COPY --chown=mysql:mysql tests/certs/ca.pem /etc/mysql/certs/ca.pem
COPY --chown=mysql:mysql tests/certs/server-cert.pem /etc/mysql/certs/server-cert.pem
COPY --chown=mysql:mysql tests/certs/server-key.pem /etc/mysql/certs/server-key.pem
RUN chmod 0644 /etc/mysql/certs/ca.pem /etc/mysql/certs/server-cert.pem \
    && chmod 0600 /etc/mysql/certs/server-key.pem
COPY docker/binflux.cnf /etc/mysql/conf.d/binflux.cnf
