# Memcache Server
Memcache Server is a Java implementation of a lightweight and concurrent Memcached server. This server provides a caching solution for key-value pairs, supporting essential Memcached commands such as SET, GET, ADD, REPLACE, CAS, DELETE, INCREMENT, DECREMENT, APPEND, and PREPEND.

# Features
* Concurrency: Concurrently processes client requests using a thread pool to ensure optimal performance.

* Command Support: Implements essential Memcached commands for storing, retrieving, and manipulating data in the cache.

* Eviction Policy: Utilizes a simple eviction policy to manage the cache size, evicting the oldest entries when the cache reaches its maximum size.

## Getting Started
Clone the Repository:
Copy code
git clone <repository-url>

# Run the Server:
Copy code
cd memcache-server
javac MemcacheServer.java
java MemcacheServer

# Connect with Telnet:
Use Telnet or a Memcached client to interact with the server and test various commands.

# Usage
Connect to the server using Telnet or a Memcached client on the specified port.

Send commands to store, retrieve, and manipulate data in the cache.

# Contribution
Contributions are welcome! Feel free to submit issues, feature requests, or pull requests to help improve this Memcache Server.

Enjoy caching with Memcache Server!
