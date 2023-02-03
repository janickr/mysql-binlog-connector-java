package com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.gtid;

import java.util.UUID;

public class Gtid {
    private final UUID serverId;
    private final long transactionId;

    public Gtid(UUID serverId, long transactionId) {
        this.serverId = serverId;
        this.transactionId = transactionId;
    }

    public static Gtid fromString(String gtid) {
        String[] split = gtid.split(":");
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        return new Gtid(UUID.fromString(sourceId), transactionId);
    }

    @Override
    public String toString() {
        return serverId.toString()+":"+transactionId;
    }

    public UUID getServerId() {
        return serverId;
    }

    public long getTransactionId() {
        return transactionId;
    }
}
