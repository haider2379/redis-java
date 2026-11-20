import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisStream {
    public record StreamEntry(StreamId id, Map<String, String> fields) {}

    private final List<StreamEntry> entries = new ArrayList<>();

    public void add(StreamEntry entry) {
        entries.add(entry);
    }

    public List<StreamEntry> entries() {
        return entries;
    }
}