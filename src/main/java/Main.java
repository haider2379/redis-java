import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Logs from your program will appear here!");

        CommandHandler handler = new CommandHandler(); // one shared instance = one shared store

        try (ServerSocket server = new ServerSocket(6379)) {
            server.setReuseAddress(true);
            while (true) {
                Socket client = server.accept();
                Thread.startVirtualThread(() -> serve(client, handler));
            }
        }
    }

    private static void serve(Socket client, CommandHandler handler) {
        try (client;
             InputStream in = new BufferedInputStream(client.getInputStream());
             OutputStream out = client.getOutputStream()) {

            List<String> command;
            while ((command = Resp.readCommand(in)) != null) {
                out.write(handler.handle(command));
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
        }
    }
}