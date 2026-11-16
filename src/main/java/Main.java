import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
public class Main {
    private static final int PORT = 6379;
    private static CommandHandler commandHandler = new CommandHandler();
    public static void main(String[] args) throws IOException {
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(PORT));
        serverChannel.configureBlocking(false);
        Selector selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("Redis server listening on port " + PORT);
        while (true) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();
                if (key.isAcceptable()) {
                    SocketChannel clientChannel = serverChannel.accept();
                    clientChannel.configureBlocking(false);
                    clientChannel.register(selector, SelectionKey.OP_READ);
                } else if (key.isReadable()) {
                    SocketChannel clientChannel = (SocketChannel) key.channel();
                    try {
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int bytesRead = clientChannel.read(buffer);
                        if (bytesRead == -1) {
                            clientChannel.close();
                            continue;
                        }
                        String input = new String(buffer.array(), 0, bytesRead).trim();
                        String[] tokens = parseRESP(input);
                        if (tokens != null && tokens.length > 0) {
                            String response = commandHandler.handle(tokens);
                            clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                        }
                    } catch (IOException e) {
                        clientChannel.close();
                    }
                }
            }
        }
    }
    private static String[] parseRESP(String input) {
        if (input.startsWith("*")) {
            String[] lines = input.split("\r\n");
            int numElements = Integer.parseInt(lines[0].substring(1));
            String[] tokens = new String[numElements];
            for (int i = 0; i < numElements; i++) {
                tokens[i] = lines[i * 2 + 2];
            }
            return tokens;
        } else {
            return input.split(" ");
        }
    }
}