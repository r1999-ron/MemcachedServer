import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    private final int maxCacheSize = 100; // Maximum cache size
    private final Lock cacheLock = new ReentrantLock();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private boolean isShuttingDown = false;

    /**
     * Constructs a MemcacheServer with the specified port.
     *
     * @param port The port on which the server will listen for incoming
     *             connections.
     */
    public MemcacheServer(int port) {
        this.port = port;
        this.cache = new LinkedHashMap<>(maxCacheSize, 0.75f, true); // LRU Cache
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
            try {
                isShuttingDown = true;
                System.out.println("Shutting down the Memcached Server gracefully...");
                // stop accepting new requests
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close(); // Close the serverSocket
                }
                // Allow ongoing requests to complete
                executorService.shutdown();
                // Optionally wait for some time for ongoing requests to complete
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    // forcefully terminate if waiting tasks take too long
                    executorService.shutdownNow();
                }
                // Additional cleanup tasks if needed
                System.out.println("Memcached Server shutdown completed");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleReplace(String[] tokens, BufferedReader reader, PrintWriter writer) {
        String key = tokens[1];
        cacheLock.lock();
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            cacheLock.unlock();
        }
    }

    private String readValueFromInputStream(BufferedReader reader, int byteCount) throws IOException {
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < byteCount; i++) {
            int charCode = reader.read();
            if (charCode == -1) {
                throw new IOException("Unexpected end of input stream");
            }
            value.append((char) charCode);
        }
        reader.readLine(); // Read and discard the trailing newline
        return value.toString().trim();
    }

    private void handleAdd(String[] tokens, BufferedReader reader, PrintWriter writer) {
        String key = tokens[1];
        cacheLock.lock();
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleSet(String[] tokens, BufferedReader reader) {
        String key = tokens[1];
        cacheLock.lock();
        try {
            int flags = Integer.parseInt(tokens[2]);
            int exptime = Integer.parseInt(tokens[3]);
            int byteCount = Integer.parseInt(tokens[4]);
            String value = readValueFromInputStream(reader, byteCount);

            if (cache.size() >= maxCacheSize) {
                evictOldestEntry();
            }

            CacheEntry entry = new CacheEntry(value, flags, exptime);
            cache.put(key, entry);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleGet(String[] tokens, PrintWriter writer) {
        String key = tokens[1];
        cacheLock.lock();
        try {
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

    private void handleAppendPrepend(String[] tokens, BufferedReader reader, PrintWriter writer, String command) {
        String key = tokens[1];
        cacheLock.lock();
        try {
            if (cache.containsKey(key)) {
                CacheEntry entry = cache.get(key);
                int byteCount = Integer.parseInt(tokens[4]);
                String value = readValueFromInputStream(reader, byteCount);

                if (command.equals("append")) {
                    entry.setValue(entry.getValue() + value);
                } else {
                    entry.setValue(value + entry.getValue());
                }
                writer.println("STORED");
            } else {
                writer.println("NOT_STORED");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleCas(String[] tokens, BufferedReader reader, PrintWriter writer) {
        String key = tokens[1];
        cacheLock.lock();
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            cacheLock.unlock();
        }
    }

    private void handleDelete(String[] tokens, PrintWriter writer) {
        String key = tokens[1];
        cacheLock.lock();
        try {
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
        String key = tokens[1];
        cacheLock.lock();
        try {
            if (cache.containsKey(key)) {
                CacheEntry entry = cache.get(key);
                int incrementValue = Integer.parseInt(tokens[2]);

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
        String oldestKey = cache.entrySet().iterator().next().getKey();
        cache.remove(oldestKey);
    }

    /**
     * Represents a cache entry in the Memcached server.
     * Each entry includes the stored value, flags, expiration time, CAS unique
     * identifier,
     * and the creation time.
     */
    private static class CacheEntry {
        private String value;
        private final int flags;
        private final int exptime;
        private final long casUnique;
        private final long creationTime;

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
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        int port = ServerConfig.INSTANCE.getPort();
        if (args.length > 0 && args[0].equals("-p") && args.length > 1) {
            try {
                // Parsing the port number from the command line
                port = Integer.parseInt(args[1]);

            } catch (NumberFormatException e) {
                System.err.println("Invalid port number. Using the default port: " + port);
            }
        }
        MemcacheServer memcachedServer = new MemcacheServer(port);
        memcachedServer.startServer();
    }
}
}