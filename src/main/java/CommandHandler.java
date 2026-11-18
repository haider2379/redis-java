import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommandHandler {
    private static final long NO_EXPIRY = -1;

    // value is either a String or a List<String>
    private record Entry(Object value, long expiresAtMs) {
        boolean isExpired() {
            return expiresAtMs != NO_EXPIRY && System.currentTimeMillis() >= expiresAtMs;
        }
    }

    private final Map<String, Entry> store = new ConcurrentHashMap<>();

    public byte[] handle(List<String> cmd) {
        if (cmd.isEmpty()) return Resp.error("empty command");

        String name = cmd.get(0).toUpperCase();
        return switch (name) {
            case "LRANGE" -> lrange(cmd);
            case "PING"  -> Resp.simple("PONG");
            case "ECHO"  -> echo(cmd);
            case "SET"   -> set(cmd);
            case "GET"   -> get(cmd);
            case "RPUSH" -> rpush(cmd);
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

    @SuppressWarnings("unchecked")
    private byte[] rpush(List<String> cmd) {
        if (cmd.size() < 3) return wrongArgs("rpush");

        String key = cmd.get(1);
        List<String> elements = cmd.subList(2, cmd.size());

        int[] newSize = new int[1];
        boolean[] wrongType = new boolean[1];

        // compute() runs atomically per key, so two clients pushing at once can't corrupt the list
        store.compute(key, (k, existing) -> {
            Entry current = (existing != null && !existing.isExpired()) ? existing : null;

            if (current == null) {
                List<String> list = new ArrayList<>(elements);
                newSize[0] = list.size();
                return new Entry(list, NO_EXPIRY);
            }
            if (current.value() instanceof List<?> list) {
                ((List<String>) list).addAll(elements);
                newSize[0] = list.size();
                return current;
            }
            wrongType[0] = true;
            return current;
        });

        return wrongType[0] ? Resp.wrongType() : Resp.integer(newSize[0]);
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

    private byte[] wrongArgs(String command) {
        return Resp.error("wrong number of arguments for '" + command + "' command");
    }
}