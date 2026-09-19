CREATE TABLE ingress_quota_global (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    messages bigint NOT NULL CHECK (messages >= 0),
    bytes bigint NOT NULL CHECK (bytes >= 0)
);
INSERT INTO ingress_quota_global(singleton,messages,bytes)
SELECT true,count(*),coalesce(sum(charge),0) FROM ingress_messages;

CREATE TABLE ingress_quota_tenants (
    tenant_id text PRIMARY KEY,
    messages bigint NOT NULL CHECK (messages >= 0),
    bytes bigint NOT NULL CHECK (bytes >= 0)
);
INSERT INTO ingress_quota_tenants(tenant_id,messages,bytes)
SELECT tenant_id,count(*),sum(charge) FROM ingress_messages GROUP BY tenant_id;

CREATE TABLE ingress_quota_devices (
    tenant_id text NOT NULL,
    product_id text NOT NULL,
    device_id text NOT NULL,
    messages bigint NOT NULL CHECK (messages >= 0),
    bytes bigint NOT NULL CHECK (bytes >= 0),
    PRIMARY KEY(tenant_id,product_id,device_id)
);
INSERT INTO ingress_quota_devices(tenant_id,product_id,device_id,messages,bytes)
SELECT tenant_id,product_id,device_id,count(*),sum(charge)
FROM ingress_messages GROUP BY tenant_id,product_id,device_id;

CREATE TABLE command_quota_global (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    commands bigint NOT NULL CHECK (commands >= 0)
);
INSERT INTO command_quota_global(singleton,commands)
SELECT true,count(*) FROM commands;

CREATE TABLE command_quota_tenants (
    tenant_id text PRIMARY KEY,
    commands bigint NOT NULL CHECK (commands >= 0)
);
INSERT INTO command_quota_tenants(tenant_id,commands)
SELECT tenant_id,count(*) FROM commands GROUP BY tenant_id;

CREATE TABLE command_quota_devices (
    tenant_id text NOT NULL,
    product_id text NOT NULL,
    device_id text NOT NULL,
    commands bigint NOT NULL CHECK (commands >= 0),
    PRIMARY KEY(tenant_id,product_id,device_id)
);
INSERT INTO command_quota_devices(tenant_id,product_id,device_id,commands)
SELECT tenant_id,product_id,device_id,count(*)
FROM commands GROUP BY tenant_id,product_id,device_id;
