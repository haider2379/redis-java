/** State that belongs to one client connection (as opposed to the shared data store). */
public class ClientSession {
    private boolean inTransaction;

    public boolean inTransaction() {
        return inTransaction;
    }

    public void startTransaction() {
        inTransaction = true;
    }
}