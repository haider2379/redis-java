import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.LinkedHashMap;

public class CommandHandler {
    private static final long NO_EXPIRY = -1;

    // value is either a String or a List<String>
    private record Entry(Object value, long expiresAtMs) {
        boolean isExpired() {
            return expiresAtMs != NO_EXPIRY && System.currentTimeMillis() >= expiresAtMs;
        }
    }

    private final Map<String, Entry> store = new ConcurrentHashMap<>();
    // key -> clients blocked on that list, oldest first
    private final Map<String, Queue<CompletableFuture<String>>> waiters = new ConcurrentHashMap<>();

    public byte[] handle(List<String> cmd) {
        if (cmd.isEmpty()) return Resp.error("empty command");

        String name = cmd.get(0).toUpperCase();
        return switch (name) {
            case "XADD" -> xadd(cmd);
            case "TYPE" -> type(cmd);
            case "BLPOP" -> blpop(cmd);
            case "LPOP" -> lpop(cmd);
            case "LLEN" -> llen(cmd);
            case "LPUSH" -> push(cmd, true);
            case "LRANGE" -> lrange(cmd);
            case "PING"  -> Resp.simple("PONG");
            case "ECHO"  -> echo(cmd);
            case "SET"   -> set(cmd);
            case "GET"   -> get(cmd);
            case "RPUSH" -> push(cmd, false);
            default      -> Resp.error("unknown command '" + cmd.get(0) + "'");
        };
    }

    private byte[] echo(List<String> cmd) {
        if (cmd.size() < 2) return wrongArgs("echo");
        return Resp.bulk(cmd.get(1));
    }

    private byte[] set(List<String> cmd) {
        if (cmd.size() < 3) return wrongArgs("set");

        long expiresAt = NO_EXPIRY;

        for (int i = 3; i < cmd.size(); i++) {
            String option = cmd.get(i).toUpperCase();
            if (option.equals("PX") || option.equals("EX")) {
                if (i + 1 >= cmd.size()) return Resp.error("syntax error");
                try {
                    long amount = Long.parseLong(cmd.get(i + 1));
                    if (amount <= 0) return Resp.error("invalid expire time in 'set' command");
                    long millis = option.equals("EX") ? amount * 1000 : amount;
                    expiresAt = System.currentTimeMillis() + millis;
                } catch (NumberFormatException e) {
                    return Resp.error("value is not an integer or out of range");
                }
                i++;
            } else {
                return Resp.error("syntax error");
            }
        }

        store.put(cmd.get(1), new Entry(cmd.get(2), expiresAt));
        return Resp.simple("OK");
    }

    private byte[] get(List<String> cmd) {
        if (cmd.size() < 2) return wrongArgs("get");

        String key = cmd.get(1);
        Entry entry = store.get(key);
        if (entry == null) return Resp.nullBulk();

        if (entry.isExpired()) {
            store.remove(key, entry);
            return Resp.nullBulk();
        }
        if (!(entry.value() instanceof String s)) return Resp.wrongType();
        return Resp.bulk(s);
    }

    private byte[] type(List<String> cmd) {
        if (cmd.size() != 2) return wrongArgs("type");

        Entry entry = store.get(cmd.get(1));
        if (entry == null || entry.isExpired()) return Resp.simple("none");

        if (entry.value() instanceof String) return Resp.simple("string");
        if (entry.value() instanceof List<?>) return Resp.simple("list");
        if (entry.value() instanceof RedisStream) return Resp.simple("stream");

        return Resp.simple("none"); // streams will get their own branch in a later stage
    }

    @SuppressWarnings("unchecked")
    private byte[] push(List<String> cmd, boolean left) {
        if (cmd.size() < 3) return wrongArgs(left ? "lpush" : "rpush");

        String key = cmd.get(1);
        List<String> elements = cmd.subList(2, cmd.size());

        int[] newSize = new int[1];
        boolean[] wrongType = new boolean[1];

        store.compute(key, (k, existing) -> {
            Entry current = (existing != null && !existing.isExpired()) ? existing : null;

            List<String> list;
            if (current == null) {
                list = new ArrayList<>();
                current = new Entry(list, NO_EXPIRY);
            } else if (current.value() instanceof List<?> l) {
                list = (List<String>) l;
            } else {
                wrongType[0] = true;
                return current;
            }

            if (left) {
                for (String element : elements) {
                    list.add(0, element); // each element goes to the front, one at a time
                }
            } else {
                list.addAll(elements);
            }

            newSize[0] = list.size();          // RPUSH reports the length *before* blocked clients take elements
            serveWaiters(key, list);
            return list.isEmpty() ? null : current;
        });

        return wrongType[0] ? Resp.wrongType() : Resp.integer(newSize[0]);
    }

    private byte[] xadd(List<String> cmd) {
        if (cmd.size() < 5 || cmd.size() % 2 == 0) return wrongArgs("xadd");

        String key = cmd.get(1);
        String idArg = cmd.get(2);

        // Work out which of the three ID formats we were given
        boolean autoTime = idArg.equals("*");            // "*"
        boolean autoSeq = idArg.endsWith("-*");          // "1526919030474-*"
        long autoSeqMs = 0;
        StreamId explicitId = null;

        try {
            if (autoSeq) {
                autoSeqMs = Long.parseLong(idArg.substring(0, idArg.length() - 2));
            } else if (!autoTime) {
                explicitId = StreamId.parse(idArg);
                if (explicitId.ms() == 0 && explicitId.seq() == 0) {
                    return Resp.error("The ID specified in XADD must be greater than 0-0");
                }
            }
        } catch (IllegalArgumentException e) {           // includes NumberFormatException
            return Resp.error("Invalid stream ID specified as stream command argument");
        }

        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 3; i < cmd.size(); i += 2) {
            fields.put(cmd.get(i), cmd.get(i + 1));
        }

        final boolean fAutoTime = autoTime, fAutoSeq = autoSeq;
        final long fAutoSeqMs = autoSeqMs;
        final StreamId fExplicit = explicitId;

        byte[][] failure = new byte[1][];
        StreamId[] resolved = new StreamId[1];

        store.compute(key, (k, existing) -> {
            Entry current = (existing != null && !existing.isExpired()) ? existing : null;

            RedisStream stream;
            if (current == null) {
                stream = new RedisStream();
                current = new Entry(stream, NO_EXPIRY);
            } else if (current.value() instanceof RedisStream s) {
                stream = s;
            } else {
                failure[0] = Resp.wrongType();
                return current;
            }

            StreamId last = stream.lastId();
            StreamId id;

            if (fAutoTime) {
                long now = System.currentTimeMillis();
                if (last != null && last.ms() >= now) {
                    id = new StreamId(last.ms(), last.seq() + 1);   // clock didn't advance: bump the sequence
                } else {
                    id = new StreamId(now, 0);
                }
            } else if (fAutoSeq) {
                long seq;
                if (last != null && last.ms() == fAutoSeqMs) {
                    seq = last.seq() + 1;                           // same time part: last sequence + 1
                } else {
                    seq = (fAutoSeqMs == 0) ? 1 : 0;                // new time part: 0, or 1 when time is 0
                }
                id = new StreamId(fAutoSeqMs, seq);
            } else {
                id = fExplicit;
            }

            // Same rule for all three formats: the new ID must beat the last one
            if (last != null && id.compareTo(last) <= 0) {
                failure[0] = Resp.error("The ID specified in XADD is equal or smaller than the target stream top item");
                return current;
            }

            stream.add(new RedisStream.StreamEntry(id, fields));
            resolved[0] = id;
            return current;
        });

        return failure[0] != null ? failure[0] : Resp.bulk(resolved[0].toString());
    }

    @SuppressWarnings("unchecked")
    private byte[] lrange(List<String> cmd) {
        if (cmd.size() != 4) return wrongArgs("lrange");

        long start, stop;
        try {
            start = Long.parseLong(cmd.get(2));
            stop = Long.parseLong(cmd.get(3));
        } catch (NumberFormatException e) {
            return Resp.error("value is not an integer or out of range");
        }

        byte[][] reply = new byte[1][];

        // compute() holds the key's lock, so we never read a list while RPUSH is changing it
        store.compute(cmd.get(1), (k, existing) -> {
            if (existing == null || existing.isExpired()) {
                reply[0] = Resp.array(List.of());
                return existing;
            }
            if (!(existing.value() instanceof List<?> list)) {
                reply[0] = Resp.wrongType();
                return existing;
            }

            int len = list.size();
            long from = start < 0 ? Math.max(len + start, 0) : start;
            long to = stop < 0 ? len + stop : Math.min(stop, len - 1);

            if (from >= len || from > to) {
                reply[0] = Resp.array(List.of());
            } else {
                List<String> slice = new ArrayList<>();
                for (long i = from; i <= to; i++) {
                    slice.add(((List<String>) list).get((int) i));
                }
                reply[0] = Resp.array(slice);
            }
            return existing;
        });

        return reply[0];
    }

    @SuppressWarnings("unchecked")
    private byte[] blpop(List<String> cmd) {
        if (cmd.size() != 3) return wrongArgs("blpop");

        String key = cmd.get(1);
        double seconds;
        try {
            seconds = Double.parseDouble(cmd.get(2));
        } catch (NumberFormatException e) {
            return Resp.error("timeout is not a float or out of range");
        }
        if (seconds < 0) return Resp.error("timeout is negative");

        CompletableFuture<String> waiter = new CompletableFuture<>();
        String[] immediate = new String[1];
        boolean[] wrongType = new boolean[1];

        // Check-and-register is atomic with respect to pushes on the same key
        store.compute(key, (k, existing) -> {
            if (existing != null && !existing.isExpired()) {
                if (!(existing.value() instanceof List<?> l)) {
                    wrongType[0] = true;
                    return existing;
                }
                List<String> list = (List<String>) l;
                if (!list.isEmpty()) {
                    immediate[0] = list.remove(0);            // data already there: no need to block
                    return list.isEmpty() ? null : existing;
                }
            }
            waiters.computeIfAbsent(key, x -> new ConcurrentLinkedQueue<>()).add(waiter);
            return existing;
        });

        if (wrongType[0]) return Resp.wrongType();
        if (immediate[0] != null) return Resp.array(List.of(key, immediate[0]));

        try {
            String element = (seconds == 0)
                    ? waiter.get()                                              // 0 = wait forever
                    : waiter.get((long) (seconds * 1000), TimeUnit.MILLISECONDS);
            return Resp.array(List.of(key, element));

        } catch (TimeoutException e) {
            // A push may have delivered at the last instant. complete(null) tells us who won the race.
            if (waiter.complete(null)) {
                Queue<CompletableFuture<String>> queue = waiters.get(key);
                if (queue != null) queue.remove(waiter);
                return Resp.nullArray();                                       // timed out: *-1\r\n
            }
            return Resp.array(List.of(key, waiter.getNow(null)));              // delivered just in time

        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            return Resp.error("interrupted");
        }
    }

    @SuppressWarnings("unchecked")
    private byte[] lpop(List<String> cmd) {
        if (cmd.size() < 2 || cmd.size() > 3) return wrongArgs("lpop");

        boolean hasCount = cmd.size() == 3;
        int count = 1;
        if (hasCount) {
            try {
                count = Integer.parseInt(cmd.get(2));
            } catch (NumberFormatException e) {
                return Resp.error("value is out of range, must be positive");
            }
            if (count < 0) return Resp.error("value is out of range, must be positive");
        }

        final int n = count;
        byte[][] reply = new byte[1][];

        store.compute(cmd.get(1), (k, existing) -> {
            if (existing == null || existing.isExpired()) {
                reply[0] = hasCount ? Resp.nullArray() : Resp.nullBulk();
                return null;
            }
            if (!(existing.value() instanceof List<?> l)) {
                reply[0] = Resp.wrongType();
                return existing;
            }

            List<String> list = (List<String>) l;

            if (!hasCount) {
                reply[0] = Resp.bulk(list.remove(0));
            } else {
                int toRemove = Math.min(n, list.size());   // asking for more than exists is fine
                List<String> removed = new ArrayList<>(list.subList(0, toRemove));
                list.subList(0, toRemove).clear();
                reply[0] = Resp.array(removed);
            }

            return list.isEmpty() ? null : existing;       // delete the key when the list empties
        });

        return reply[0];
    }

    private void serveWaiters(String key, List<String> list) {
        Queue<CompletableFuture<String>> queue = waiters.get(key);
        if (queue == null) return;

        while (!list.isEmpty()) {
            CompletableFuture<String> waiter = queue.poll();   // oldest first
            if (waiter == null) break;
            // complete() fails if that waiter already timed out; then the element stays and we try the next waiter
            if (waiter.complete(list.get(0))) list.remove(0);
        }
    }

    private byte[] llen(List<String> cmd) {
        if (cmd.size() != 2) return wrongArgs("llen");

        Entry entry = store.get(cmd.get(1));
        if (entry == null || entry.isExpired()) return Resp.integer(0);
        if (!(entry.value() instanceof List<?> list)) return Resp.wrongType();

        return Resp.integer(list.size());
    }

    private byte[] wrongArgs(String command) {
        return Resp.error("wrong number of arguments for '" + command + "' command");
    }
}