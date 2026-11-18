import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.util.List;

public final class Resp {
    private Resp() {}

    // ---------- Encoding ----------

    public static byte[] simple(String s) {
        return ("+" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] error(String s) {
        return ("-ERR " + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] bulk(String s) {
        int len = s.getBytes(StandardCharsets.UTF_8).length; // bytes, not chars
        return ("$" + len + "\r\n" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] nullBulk() {
        return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] nullArray() {
        return "*-1\r\n".getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] integer(long n) {
        return (":" + n + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] array(List<String> items) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String item : items) {
            out.writeBytes(bulk(item));
        }
        return out.toByteArray();
    }

    public static byte[] wrongType() {
        // Real Redis uses the WRONGTYPE prefix instead of the generic ERR
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                .getBytes(StandardCharsets.UTF_8);
    }

    // ---------- Parsing ----------

    /** Reads one command (an array of bulk strings). Returns null when the client disconnects. */
    public static List<String> readCommand(InputStream in) throws IOException {
        int prefix = in.read();
        if (prefix == -1) return null;
        if (prefix != '*') throw new IOException("Expected '*' but got '" + (char) prefix + "'");

        int count = Integer.parseInt(readLine(in));
        List<String> parts = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            int type = in.read();
            if (type != '$') throw new IOException("Expected '$' but got " + type);

            int len = Integer.parseInt(readLine(in));
            byte[] data = in.readNBytes(len);
            if (data.length != len) throw new EOFException("Connection closed mid-command");
            in.readNBytes(2); // trailing \r\n

            parts.add(new String(data, StandardCharsets.UTF_8));
        }
        return parts;
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read(); // '\n'
                return sb.toString();
            }
            sb.append((char) b);
        }
        throw new EOFException();
    }
}