import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/**
 * MemcacheServer is a simple implementation of a Memcached server.
 * It supports basic Memcached commands such as SET, GET, ADD, REPLACE, etc.
 *
 * The server listens on a specified port for incoming client connections
 * and processes client requests concurrently using a thread pool.
 */

public class MemcacheServer {

    private final int port;
    private Map<String, CacheEntry> cache;
    private int maxCacheSize = 100; // Maximum cache size
    private final Lock cacheLock = new ReentrantLock();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private volatile boolean isShuttingDown = false;

    /**
     * Constructs a MemcacheServer with the specified port.
     *
     * @param port The port on which the server will listen for incoming
     *             connections.
     */
    public MemcacheServer(int port) {
        this.port = port;
        this.cache = new HashMap<>();
    }

    /**
     * Starts the Memcached server. It listens for incoming client connections
     * and handles each client request concurrently using a thread pool.
     */
    public void startServer() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Memcached server is listening on port " + port);

            while (!isShuttingDown) {
                Socket clientSocket = serverSocket.accept();
                if (!isShuttingDown) {
                    System.out.println("Client Connected");
                    executorService.submit(() -> handleClient(clientSocket));
                } else {
                    clientSocket.close();
                }

            }
        } catch (IOException e) {
            if (!isShuttingDown) {
                e.printStackTrace();
            }

        } finally {
            shutdown();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
            // Print client address
            System.out.println("Client Address: " + clientSocket.getInetAddress());
            String request = reader.readLine();
            if (request != null && !request.isEmpty()) {
                System.out.println("Received request: " + request);
                String[] tokens = request.split(" ");
                String command = tokens[0].toLowerCase();

                switch (command) {
                    case "set":
                        handleSet(tokens, reader);
                        writer.println("STORED");
                        break;
                    case "get":
                        handleGet(tokens, writer);
                        break;
                    case "append":
                    case "prepend":
                        handleAppendPrepend(tokens, reader, writer, command);
                        break;
                    case "cas":
                        handleCas(tokens, reader, writer);
                        break;
                    case "delete":
                        handleDelete(tokens, writer);
                        break;
                    case "increment":
                    case "decrement":
                        handleIncrementDecrement(tokens, writer, command);
                        break;
                    case "add":
                        handleAdd(tokens, reader, writer);
                        break;
                    case "replace":
                        handleReplace(tokens, reader, writer);
                        break;
                    default:
                        writer.println("ERROR");
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close(); // close the clientSocket
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void shutdown() {
        if (!isShuttingDown) {
            isShuttingDown = true;
            System.out.println("Shutting down the Memcached Server gracefully...");
            try {
                // stop accepting new requests
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close(); // Close the serverSocket
                }
                // Allow ongoing requests to complete
                executorService.shutdownNow();
                // Optionally wait for some time for ongoing requests to complete
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    // forcefully terminate if waiting tasks takes too long
                    executorService.shutdownNow();
                }
                // Additional cleanup tasks if needed
                System.out.println("Memcached Server shutdown completed");

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();

            }
        }
    }

    private void handleReplace(String[] tokens, BufferedReader reader, PrintWriter writer) throws IOException {
        cacheLock.lock();
        try {
            String key = tokens[1];
            if (cache.containsKey(key)) {
                int flags = Integer.parseInt(tokens[2]);
                int exptime = Integer.parseInt(tokens[3]);
                int byteCount = Integer.parseInt(tokens[4]);
                String value = readValueFromInputStream(reader, byteCount);

                CacheEntry entry = new CacheEntry(value, flags, exptime);
                cache.put(key, entry);
                writer.println("STORED");
            } else {
                writer.println("NOT_STORED");
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private String readValueFromInputStream(BufferedReader reader, int byteCount) throws IOException {
        String value = IntStream.range(0, byteCount).mapToObj(i -> {
            try {
                int charCode = reader.read();
                if (charCode == -1) {
                    throw new IOException("Unexpected end of input stream");
                }
                return (char) charCode;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                .toString();
        reader.readLine();
        return value.trim();
    }

    private void handleAdd(String[] tokens, BufferedReader reader, PrintWriter writer) throws IOException {
        cacheLock.lock();
        try {
            String key = tokens[1];
            if (!cache.containsKey(key)) {
                int flags = Integer.parseInt(tokens[2]);
                int exptime = Integer.parseInt(tokens[3]);
                int byteCount = Integer.parseInt(tokens[4]);
                String value = readValueFromInputStream(reader, byteCount);
                if (cache.size() >= maxCacheSize) {
                    evictOldestEntry();
                }

                CacheEntry entry = new CacheEntry(value, flags, exptime);
                cache.put(key, entry);
                writer.println("STORED");
            } else {
                writer.println("NOT_STORED");
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleSet(String[] tokens, BufferedReader reader) throws IOException {
        cacheLock.lock();
        try {
            String key = tokens[1];
            int flags = Integer.parseInt(tokens[2]);
            int exptime = Integer.parseInt(tokens[3]);
            int byteCount = Integer.parseInt(tokens[4]);
            String value = readValueFromInputStream(reader, byteCount);

            if (cache.size() >= maxCacheSize) {
                evictOldestEntry();
            }

            CacheEntry entry = new CacheEntry(value, flags, exptime);
            cache.put(key, entry);
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleGet(String[] tokens, PrintWriter writer) {
        cacheLock.lock();
        try {
            String key = tokens[1];
            if (cache.containsKey(key)) {
                CacheEntry entry = cache.get(key);
                writer.println("VALUE " + key + " " + entry.getFlags() + " " + entry.getValue().length());
                writer.println(entry.getValue());
                writer.println("END");
            } else {
                writer.println("END");
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleAppendPrepend(String[] tokens, BufferedReader reader, PrintWriter writer, String command)
            throws IOException {
        cacheLock.lock();
        try {
            String key = tokens[1];
            int byteCount = Integer.parseInt(tokens[4]);
            String value = readValueFromInputStream(reader, byteCount);

            if (cache.containsKey(key)) {
                CacheEntry entry = cache.get(key);
                if (command.equals("append")) {
                    entry.setValue(entry.getValue() + value);
                } else { // prepend
                    entry.setValue(value + entry.getValue());
                }
                writer.println("STORED");
            } else {
                writer.println("NOT_STORED");
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleCas(String[] tokens, BufferedReader reader, PrintWriter writer) throws IOException {
        cacheLock.lock();
        try {
            String key = tokens[1];
            long casUnique = Long.parseLong(tokens[5]);
            int byteCount = Integer.parseInt(tokens[6]);
            String value = readValueFromInputStream(reader, byteCount);
            if (cache.containsKey(key) && cache.get(key).getCasUnique() == casUnique) {
                CacheEntry entry = cache.get(key);
                entry.setValue(value);
                writer.println("STORED");
            } else {
                writer.println("EXISTS");
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleDelete(String[] tokens, PrintWriter writer) {
        cacheLock.lock();
        try {
            String key = tokens[1];
            if (cache.containsKey(key)) {
                cache.remove(key);
                writer.println("DELETED");
            } else {
                writer.println("NOT_FOUND");
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleIncrementDecrement(String[] tokens, PrintWriter writer, String command) {
        cacheLock.lock();
        try {
            String key = tokens[1];
            int incrementValue = Integer.parseInt(tokens[2]);

            if (cache.containsKey(key)) {
                CacheEntry entry = cache.get(key);
                int oldValue = Integer.parseInt(entry.getValue());
                int newValue = (command.equals("increment")) ? oldValue + incrementValue : oldValue - incrementValue;
                entry.setValue(Integer.toString(newValue));
                writer.println(newValue);
            } else {
                writer.println("NOT_FOUND");
            }
        } finally {
            cacheLock.unlock();
        }
    }

    private void evictOldestEntry() {
        cacheLock.lock();
        try {
            String oldestKey = null;
            long oldestTime = Long.MAX_VALUE;

            for (Map.Entry<String, CacheEntry> entry : cache.entrySet()) {
                if (entry.getValue().getCreationTime() < oldestTime) {
                    oldestTime = entry.getValue().getCreationTime();
                    oldestKey = entry.getKey();
                }
            }

            if (oldestKey != null) {
                cache.remove(oldestKey);
            }
        } finally {
            cacheLock.unlock();

        }
    }

    /**
     * Represents a cache entry in the Memcached server.
     * Each entry includes the stored value, flags, expiration time, CAS unique
     * identifier,
     * and the creation time.
     */
    private static class CacheEntry {
        private String value;
        private int flags;
        private int exptime;
        private long casUnique;
        private long creationTime;

        /**
         * Constructs a CacheEntry with the specified value, flags, and expiration time.
         *
         * @param value   The value stored in the cache entry.
         * @param flags   Flags associated with the cache entry.
         * @param exptime Expiration time of the cache entry.
         */
        public CacheEntry(String value, int flags, int exptime) {
            this.value = value;
            this.flags = flags;
            this.exptime = exptime;
            this.casUnique = System.nanoTime(); // Simple implementation for casUnique
            this.creationTime = System.currentTimeMillis();
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getFlags() {
            return flags;
        }

        public long getCasUnique() {
            return casUnique;
        }

        public long getCreationTime() {
            return creationTime;
        }

    }

    /**
     * Main method to start the Memcached server.
     *
     * @param args Command line arguments (not used in this example).
     */
    public static void main(String[] args) {
        int port = ServerConfig.INSTANCE.getPort();
        MemcacheServer memcachedServer = new MemcacheServer(port);
        memcachedServer.startServer();
    }
}