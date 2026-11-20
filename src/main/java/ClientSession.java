import java.util.ArrayList;
import java.util.List;

/** State that belongs to one client connection (as opposed to the shared data store). */
public class ClientSession {
    private boolean inTransaction;
    private final List<List<String>> queued = new ArrayList<>();

    public boolean inTransaction() {
        return inTransaction;
    }

    public void startTransaction() {
        inTransaction = true;
        queued.clear();
    }

    public void queue(List<String> command) {
        queued.add(command);
    }

    /** Ends the transaction and hands back the queued commands, in order. */
    public List<List<String>> endTransaction() {
        inTransaction = false;
        List<List<String>> result = new ArrayList<>(queued);
        queued.clear();
        return result;
    }
}