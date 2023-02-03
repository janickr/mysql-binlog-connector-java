package com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.gtid;

import java.util.UUID;

public class UuidSet {
    private final UUID serverId;
    private final TransactionIdSet transactionIdSet;

    public UuidSet(Gtid gtid) {
        this(gtid.getServerId(), new TransactionIdSet(gtid.getTransactionId()));
    }

    private UuidSet(UUID serverId, TransactionIdSet transactionIdSet) {
        this.serverId = serverId;
        this.transactionIdSet = transactionIdSet;
    }

    public void add(long transactionId) {
        transactionIdSet.add(transactionId);
    }

    public UUID getServerId() {
        return serverId;
    }

    @Override
    public String toString() {
        return serverId + ":" + transactionIdSet;
    }

    public static UuidSet fromString(String uuidSetString) {
        String[] split = uuidSetString.split(":", 2);
        return new UuidSet(UUID.fromString(split[0]), TransactionIdSet.fromString(split[1]));
    }

}
