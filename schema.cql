CREATE KEYSPACE scheduler WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE scheduler.commands (
    command_id text PRIMARY KEY,
    batch_id text,
    data blob,
    completed boolean,
    succeed boolean
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.batches (
    id text PRIMARY KEY,
    completed boolean,
    workers set<text>,
    votes set<text>,
    failures set<text>,
    successes set<text>,
    writes_completed int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.links (
    offset int PRIMARY KEY,
    data blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.batch_partitions (
    batch_id text,
    command_id text,
    scheduler int,
    PRIMARY KEY (batch_id, command_id, scheduler)
) WITH CLUSTERING ORDER BY (command_id ASC, scheduler ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.data (
    id text PRIMARY KEY,
    value int,
    version text
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.topics (
    id text PRIMARY KEY,
    state blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.topic_offsets (
    topic text,
    partition int,
    last bigint,
    offset bigint,
    PRIMARY KEY (topic, partition)
) WITH CLUSTERING ORDER BY (partition ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.aggregators (
    id text PRIMARY KEY,
    offsets blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.coordinators (
    id text PRIMARY KEY,
    offset blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.metas (
    id text PRIMARY KEY,
    data blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.log_offsets (
    consumer text PRIMARY KEY,
    log_offset bigint,
    status_offset bigint,
    state blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.batch_keys (
    batch_id text,
    scheduler int,
    data blob,
    PRIMARY KEY (batch_id, scheduler)
) WITH CLUSTERING ORDER BY (scheduler ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.scheduler_commands (
    batch_id text,
    scheduler int,
    command_id text,
    data blob,
    PRIMARY KEY (batch_id, scheduler, command_id)
) WITH CLUSTERING ORDER BY (scheduler ASC, command_id ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.partition_meta (
    topic text,
    p int,
    last_offset bigint,
    PRIMARY KEY (topic, p)
) WITH CLUSTERING ORDER BY (p ASC)
    AND default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE scheduler.workers (
    id text PRIMARY KEY,
    offset blob
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE KEYSPACE indexes WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE indexes.meta (
    name text PRIMARY KEY,
    roots blob,
    num_leaf_entries int,
    num_meta_entries int
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};

CREATE TABLE indexes.blocks (
    id text PRIMARY KEY,
    bin blob,
    leaf boolean,
    size bigint
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'false'};