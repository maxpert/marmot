CREATE TABLE IF NOT EXISTS raft_info(
    entry_index UNSIGNED BIG INT,
    node_id UNSIGNED BIG INT NOT NULL,
    cluster_id UNSIGNED BIG INT NOT NULL,
    entry_type INTEGER NOT NULL,
    payload BLOB
);

CREATE INDEX IF NOT EXISTS raft_info_tpl_index
    ON raft_info(node_id, cluster_id, entry_type);

CREATE UNIQUE INDEX IF NOT EXISTS raft_info_entry_index
    ON raft_info(entry_index, node_id, cluster_id, entry_type)
    WHERE entry_index IS NOT NULL;