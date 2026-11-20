public record StreamId(long ms, long seq) implements Comparable<StreamId> {

    /** Parses "1526919030474-0". Throws NumberFormatException or IllegalArgumentException if malformed. */
    public static StreamId parse(String s) {
        int dash = s.indexOf('-');
        if (dash < 0) throw new IllegalArgumentException("missing '-'");
        long ms = Long.parseLong(s.substring(0, dash));
        long seq = Long.parseLong(s.substring(dash + 1));
        return new StreamId(ms, seq);
    }

    @Override
    public int compareTo(StreamId other) {
        int byMs = Long.compare(ms, other.ms);
        return byMs != 0 ? byMs : Long.compare(seq, other.seq);
    }

    @Override
    public String toString() {
        return ms + "-" + seq;
    }
}