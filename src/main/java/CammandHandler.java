import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommandHandler {
    private final Map<String, String> store = new ConcurrentHashMap<>();

    /** Takes a parsed command like ["SET", "foo", "bar"] and returns the RESP reply bytes. */
    public byte[] handle(List<String> cmd) {
        if (cmd.isEmpty()) return Resp.error("empty command");

        String name = cmd.get(0).toUpperCase();
        return switch (name) {
            case "PING" -> Resp.simple("PONG");
            case "ECHO" -> echo(cmd);
            case "SET"  -> set(cmd);
            case "GET"  -> get(cmd);
            default     -> Resp.error("unknown command '" + cmd.get(0) + "'");
        };
    }

    private byte[] echo(List<String> cmd) {
        if (cmd.size() < 2) return wrongArgs("echo");
        return Resp.bulk(cmd.get(1));
    }

    private byte[] set(List<String> cmd) {
        if (cmd.size() < 3) return wrongArgs("set");
        store.put(cmd.get(1), cmd.get(2));
        return Resp.simple("OK");
    }

    private byte[] get(List<String> cmd) {
        if (cmd.size() < 2) return wrongArgs("get");
        String value = store.get(cmd.get(1));
        return value == null ? Resp.nullBulk() : Resp.bulk(value);
    }

    private byte[] wrongArgs(String command) {
        return Resp.error("wrong number of arguments for '" + command + "' command");
    }
}