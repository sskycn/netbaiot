# Test fixtures

Historical EventBus spool compatibility fixtures and their reproducible generator
are documented in [restart-spool/README.md](restart-spool/README.md).

## Test-only TLS fixture

The published private key and self-signed certificate in this directory are solely
for the TLS integration test. They are not credentials for any deployment. The
certificate is valid for localhost and 127.0.0.1 and expires in September 2036.
Regenerate both files together if needed:

```sh
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout tests/fixtures/localhost-key.pem \
  -out tests/fixtures/localhost-cert.pem -days 3650 \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -addext 'basicConstraints=critical,CA:FALSE'
```
