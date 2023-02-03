package com.github.shyiko.mysql.binlog.gtidprofilingpoc;

import com.github.shyiko.mysql.binlog.*;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.event.NewEventHeaderV4Deserializer;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.BinaryLogClientForNewGtidSet;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.MySqlGtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.TraceLifecycleListenerNew;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.originalwithoutstring.BinaryLogClientForOriginalGtidSetButWithoutString;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.originalwithoutstring.TraceLifecycleListenerWithoutString;
import one.profiler.AsyncProfiler;

import java.io.IOException;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProfileMain {

    public static final int BINLOG_EVENTS_TO_PROCESS = 1500000;
    public static final int ROWS_TO_INSERT = 300000;

    public static void main(String[] args) throws Exception {
        startMySql();
        AsyncProfiler profiler = AsyncProfiler.getInstance();
        System.out.println(profiler.execute("version"));

        startWithOriginalGtidSet();
        startWithOriginalGtidSetWithoutStrings();
        startWithNewGtidSet();
        startWithNewGtidSetAndNewEventHeaderDeserializer();

        System.out.println(profiler.execute("start,event=cpu,interval=1000"));
        startWithOriginalGtidSet();
        System.out.println(profiler.execute("stop,flamegraph,file=cpu-original.html"));

        System.out.println(profiler.execute("start,event=cpu,interval=1000"));
        startWithOriginalGtidSetWithoutStrings();
        System.out.println(profiler.execute("stop,flamegraph,file=cpu-without-strings.html"));

        System.out.println(profiler.execute("start,event=cpu,interval=1000"));
        startWithNewGtidSet();
        System.out.println(profiler.execute("stop,flamegraph,file=cpu-new.html"));

        System.out.println(profiler.execute("start,event=alloc,alloc=512b"));
        startWithOriginalGtidSet();
        System.out.println(profiler.execute("stop,total,flamegraph,file=alloc-original.html"));

        System.out.println(profiler.execute("start,event=alloc,alloc=512b"));
        startWithOriginalGtidSetWithoutStrings();
        System.out.println(profiler.execute("stop,total,flamegraph,file=alloc-without-strings.html"));

        System.out.println(profiler.execute("start,event=alloc,alloc=512b"));
        startWithNewGtidSet();
        System.out.println(profiler.execute("stop,total,flamegraph,file=alloc-new.html"));

        System.out.println(profiler.execute("start,event=alloc,alloc=512b"));
        startWithNewGtidSetAndNewEventHeaderDeserializer();
        System.out.println(profiler.execute("stop,total,flamegraph,file=alloc-new-eventheader.html"));
    }


    private static MysqlOnetimeServerOptions getOptions() {
        MysqlOnetimeServerOptions options = new MysqlOnetimeServerOptions();
        options.fullRowMetaData = true;
        options.gtid = true;

        return options;
    }

    public static void startMySql() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        MysqlOnetimeServer masterServer = new MysqlOnetimeServer(getOptions());

        masterServer.boot();

        MySQLConnection master = new MySQLConnection("127.0.0.1", masterServer.getPort(), "root", "");

        System.out.println("Adding "+ROWS_TO_INSERT+" this may take a while");

        master.execute(statement -> {
            statement.execute("drop database if exists mbcj_test");
            statement.execute("create database mbcj_test");
            statement.execute("use mbcj_test");
            statement.execute("create table mbcj_test.a_test(id int, txt varchar(255) ) engine=INNODB;");
            for (int i = 0; i < ROWS_TO_INSERT; i++) {
                if (i % 10000 == 0) {
                    System.out.println(i);
                }
                statement.execute("BEGIN");
                statement.execute("insert into mbcj_test.a_test values ("+i+", 'some long varchar value " + i +
                    " some long varchar value some long varchar value some long varchar value " +
                    "some long varchar value some long varchar value some long varchar value ')");
                statement.execute("COMMIT");
            }
        });

        System.out.println("Binary logs:\n");
        AtomicLong total = new AtomicLong();
        master.query("SHOW BINARY LOGS", rs -> {
            while (rs.next()) {
                String name = rs.getString(1);
                long size = rs.getLong(2);
                total.addAndGet(size);
                System.out.println(name + ": " + size + " bytes");
            }
        });
        System.out.println("\n");

        System.out.println("Executed gtid set:");
        master.query("SHOW MASTER STATUS", rs -> {
            while (rs.next()) {
                String status = rs.getString("Executed_Gtid_Set");
                System.out.println(status);
            }
        });

        System.out.println("\nMysql server started, data added.");
        System.out.println("Binary log size: " + total.get()/1024/1024 + " Mib");
    }


    public static void startWithOriginalGtidSet() throws Exception {
        BinaryLogClient client = new BinaryLogClient("127.0.0.1", 33306, "root", "");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG);
        client.setEventDeserializer(eventDeserializer);
        client.setServerId(client.getServerId() - 1); // avoid clashes between BinaryLogClient instances
        client.setKeepAlive(false);
        client.setGtidSet("");
        AtomicInteger events = new AtomicInteger();
        client.registerEventListener(event -> {
            if (events.incrementAndGet() == BINLOG_EVENTS_TO_PROCESS) {
                System.out.println("client.getGtidSet()");
                System.out.println(client.getGtidSet());
                System.out.flush();
                try {
                    client.disconnect();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        client.registerLifecycleListener(new TraceLifecycleListener());
        client.connect();
    }

    public static void startWithOriginalGtidSetWithoutStrings() throws Exception {
        BinaryLogClientForOriginalGtidSetButWithoutString client = new BinaryLogClientForOriginalGtidSetButWithoutString("127.0.0.1", 33306, "root", "");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG);
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new MySqlGtidEventDataDeserializer());
        client.setEventDeserializer(eventDeserializer);
        client.setServerId(client.getServerId() - 2); // avoid clashes between BinaryLogClient instances
        client.setKeepAlive(false);
        client.setGtidSet("");
        AtomicInteger events = new AtomicInteger();
        client.registerEventListener(event -> {
            if (events.incrementAndGet() == BINLOG_EVENTS_TO_PROCESS) {
                System.out.println("client.getGtidSet()");
                System.out.println(client.getGtidSet());
                System.out.flush();
                client.disconnect();
            }
        });
        client.registerLifecycleListener(new TraceLifecycleListenerWithoutString());
        client.connect();
    }

    public static void startWithNewGtidSet() throws Exception {
        BinaryLogClientForNewGtidSet client = new BinaryLogClientForNewGtidSet("127.0.0.1", 33306, "root", "");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG);
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new MySqlGtidEventDataDeserializer());
        client.setEventDeserializer(eventDeserializer);
        client.setServerId(client.getServerId() - 3); // avoid clashes between BinaryLogClient instances
        client.setKeepAlive(false);
        client.setGtidSet("");
        AtomicInteger events = new AtomicInteger();
        client.registerEventListener(event -> {
            if (events.incrementAndGet() == BINLOG_EVENTS_TO_PROCESS) {
                System.out.println("client.getGtidSet()");
                System.out.println(client.getGtidSet());
                System.out.flush();
                client.disconnect();
            }
        });
        client.registerLifecycleListener(new TraceLifecycleListenerNew());
        client.connect();
    }

    public static void startWithNewGtidSetAndNewEventHeaderDeserializer() throws Exception {
        BinaryLogClientForNewGtidSet client = new BinaryLogClientForNewGtidSet("127.0.0.1", 33306, "root", "");
        EventDeserializer eventDeserializer = new EventDeserializer(new NewEventHeaderV4Deserializer());
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG);
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new MySqlGtidEventDataDeserializer());
        client.setEventDeserializer(eventDeserializer);
        client.setServerId(client.getServerId() - 4); // avoid clashes between BinaryLogClient instances
        client.setKeepAlive(false);
        client.setGtidSet("");
        AtomicInteger events = new AtomicInteger();
        client.registerEventListener(event -> {
            if (events.incrementAndGet() == BINLOG_EVENTS_TO_PROCESS) {
                System.out.println("client.getGtidSet()");
                System.out.println(client.getGtidSet());
                System.out.flush();
                client.disconnect();
            }
        });
        client.registerLifecycleListener(new TraceLifecycleListenerNew());
        client.connect();
    }
}
