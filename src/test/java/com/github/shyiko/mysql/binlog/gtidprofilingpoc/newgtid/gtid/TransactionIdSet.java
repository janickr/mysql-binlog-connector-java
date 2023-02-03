package com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.gtid;

public class TransactionIdSet {

    public static TransactionIdSet fromString(String setString) {
        Interval previous = null;
        for (String intervalString : setString.split(":")) {
            previous = Interval.fromString(intervalString, previous);
        }

        return new TransactionIdSet(previous);
    }

    private static class Interval {
        private long startInclusive;
        private long endExclusive;

        private Interval previous;

        public Interval(long singleton) {
            this(singleton,  null);
        }

        public Interval(long startInclusive, long endExclusive, Interval previous) {
            this.startInclusive = startInclusive;
            this.endExclusive = endExclusive;
            this.previous = previous;
        }

        private Interval(long singleton, Interval previous) {
            this(singleton, singleton+1, previous);
        }

        public Interval add(long transactionId) {
            if (transactionId == endExclusive) {
                endExclusive++;
                return this;
            } else if (transactionId == startInclusive - 1) {
                startInclusive--;
                mergePreviousIfNoGap();
                return this;
            } else if (transactionId > endExclusive) {
                return new Interval(transactionId, this);
            } else if (transactionId < startInclusive) {
                previous = addToPreviousSet(transactionId, previous);
                return this;
            } else {
                return this;
            }
        }

        private static Interval addToPreviousSet(long transactionId, Interval previous) {
            return previous == null ? new Interval(transactionId) : previous.add(transactionId);
        }

        private void mergePreviousIfNoGap() {
            if (previous != null && startInclusive == previous.endExclusive) {
                startInclusive = previous.startInclusive;
                previous = previous.previous;
            }
        }

        @Override
        public String toString() {
            long endInclusive = endExclusive -1;
            String current = (startInclusive == endInclusive) ? ""+startInclusive : startInclusive+ "-" + endInclusive;
            return this.previous != null ? this.previous + ":" +current : current;
        }

        public static Interval fromString(String intervalString, Interval previous) {
            String[] split = intervalString.split("-");
            return split.length == 1
                ? new Interval(Long.parseLong(split[0]), previous)
                : new Interval(Long.parseLong(split[0]), Long.parseLong(split[1])+1, previous);
        }
    }
    private Interval lastInterval;

    public TransactionIdSet(long transactionId) {
        this(new Interval(transactionId));
    }

    public TransactionIdSet(Interval lastInterval) {
        this.lastInterval = lastInterval;
    }

    public void add(long transactionId) {
        if (transactionId <= lastInterval.endExclusive) {
            this.lastInterval.add(transactionId);
        } else {
            this.lastInterval = new Interval(transactionId, lastInterval);
        }
    }

    @Override
    public String toString() {
        return lastInterval.toString();
    }
}
