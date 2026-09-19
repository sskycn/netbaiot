CREATE TABLE tenants (tenant_id text PRIMARY KEY CHECK (length(tenant_id) BETWEEN 1 AND 64));
CREATE TABLE products (tenant_id text REFERENCES tenants, product_id text NOT NULL, codec_id text NOT NULL, codec_version integer NOT NULL, PRIMARY KEY(tenant_id,product_id));
CREATE TABLE devices (tenant_id text, product_id text, device_id text NOT NULL, PRIMARY KEY(tenant_id,product_id,device_id), FOREIGN KEY(tenant_id,product_id) REFERENCES products);
CREATE TABLE device_credentials (credential_id text PRIMARY KEY, tenant_id text, product_id text, device_id text, credential_version bigint NOT NULL, verifier bytea NOT NULL, FOREIGN KEY(tenant_id,product_id,device_id) REFERENCES devices);
CREATE TABLE ingress_messages (
    message_id uuid PRIMARY KEY, tenant_id text NOT NULL, product_id text NOT NULL, device_id text NOT NULL,
    source_message_id text NOT NULL, message jsonb NOT NULL, canonical bytea NOT NULL, charge bigint NOT NULL,
    accepted_at bigint NOT NULL, expires_at bigint NOT NULL,
    UNIQUE(tenant_id,product_id,device_id,source_message_id)
);
CREATE INDEX ingress_expiry ON ingress_messages(expires_at);
CREATE TABLE delivery_jobs (
    message_id uuid PRIMARY KEY REFERENCES ingress_messages ON DELETE CASCADE,
    attempts integer NOT NULL DEFAULT 0, next_attempt_at bigint NOT NULL, expires_at bigint NOT NULL,
    lease_owner uuid, lease_expiry bigint, last_error text, done boolean NOT NULL DEFAULT false
);
CREATE INDEX delivery_due ON delivery_jobs(next_attempt_at) WHERE NOT done;
CREATE TABLE commands (
    command_id uuid PRIMARY KEY, tenant_id text NOT NULL, product_id text NOT NULL, device_id text NOT NULL,
    record jsonb NOT NULL, next_attempt_at bigint NOT NULL, expires_at bigint NOT NULL, retain_until bigint NOT NULL,
    terminal boolean NOT NULL DEFAULT false
);
CREATE INDEX commands_device_due ON commands(tenant_id,product_id,device_id,next_attempt_at) WHERE NOT terminal;
CREATE INDEX commands_retention ON commands(retain_until);
CREATE TABLE command_attempts (
    command_id uuid REFERENCES commands ON DELETE CASCADE, attempt integer NOT NULL, attempted_at bigint NOT NULL,
    state text NOT NULL, PRIMARY KEY(command_id,attempt)
);
