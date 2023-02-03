package com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid;


import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.gtid.Gtid;

public class MySqlGtidEventData implements EventData {

    public static final byte COMMIT_FLAG = 1;

    private final Gtid gtid;
    private final byte flags;

    public MySqlGtidEventData(Gtid gtid, byte flags) {
        this.gtid = gtid;
        this.flags = flags;
    }

    public Gtid getGtid() {
        return gtid;
    }

    public byte getFlags() {
        return flags;
    }

    public String toString() {
        return "GtidEventData{flags=" + flags + ", gtid='" + gtid + "''}";
    }

}
