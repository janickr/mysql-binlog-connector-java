package com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.gtid;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MySqlGtidSet {

    private final List<UuidSet> uuidSets;
    private UuidSet currentSet;

    public MySqlGtidSet() {
        this.uuidSets = new ArrayList<>();
    }

    private MySqlGtidSet(List<UuidSet> uuidSets) {
        this.uuidSets = uuidSets;
        this.currentSet = uuidSets.get(0);
    }

    public static MySqlGtidSet fromString(String gtidSetString) {
        return new MySqlGtidSet(Stream.of(gtidSetString.split(",")).map(UuidSet::fromString).collect(Collectors.toList()));
    }

    public void add(String gtidString) {
        add(Gtid.fromString(gtidString));
    }

    public void add(Gtid gtid) {
        if (currentSet == null) {
            currentSet = new UuidSet(gtid);
            uuidSets.add(currentSet);
        } else if (hasSameServerId(gtid, currentSet)) {
            currentSet.add(gtid.getTransactionId());
        } else {
            currentSet = findSet(gtid, uuidSets);
            if (currentSet == null) {
                currentSet = new UuidSet(gtid);
                uuidSets.add(currentSet);
            } else {
                currentSet.add(gtid.getTransactionId());
            }
        }
    }
    public void addMeh(Gtid gtid) {
        if (!hasSameServerId(gtid, currentSet)) {
            currentSet = findOrCreateSet(gtid, uuidSets);
        }
        currentSet.add(gtid.getTransactionId());
    }

    private UuidSet findOrCreateSet(Gtid gtid, List<UuidSet> uuidSets) {
        UuidSet found = findSet(gtid, uuidSets);
        return found == null ? addNewSet(gtid, uuidSets) : found;
    }

    private UuidSet addNewSet(Gtid gtid, List<UuidSet> uuidSets) {
        UuidSet uuidSet = new UuidSet(gtid);
        uuidSets.add(uuidSet);
        return uuidSet;
    }

    private UuidSet findSet(Gtid gtid, List<UuidSet> sets) {
        for (UuidSet set : sets) {
            if (hasSameServerId(gtid, set)) {
                return set;
            }
        }
        return null;
    }

    private boolean hasSameServerId(Gtid gtid, UuidSet uuidSet) {
        return uuidSet.getServerId().equals(gtid.getServerId());
    }

    @Override
    public String toString() {
        return uuidSets.stream().map(UuidSet::toString).collect(Collectors.joining(","));
    }

    @Deprecated
    public List<UuidSet> getUuidSets() {
        return Collections.unmodifiableList(uuidSets);
    }
}
