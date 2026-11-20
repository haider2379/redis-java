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

    public StreamId lastId() {
        return entries.isEmpty() ? null : entries.get(entries.size() - 1).id();
    }

    public List<StreamEntry> range(StreamId start, StreamId end) {
        List<StreamEntry> result = new ArrayList<>();
        for (StreamEntry e : entries) {
            if (e.id().compareTo(start) >= 0 && e.id().compareTo(end) <= 0) {
                result.add(e);
            }
        }
        return result;
    }
}