# Build Your Own Redis (Java)

A Redis server written in Java from scratch, built while working through the
[CodeCrafters "Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis).
It speaks the real Redis wire protocol (RESP), so you can talk to it with the
standard `redis-cli`.

## Requirements

- Java 21 or newer (client connections run on virtual threads)
- Maven

## Running it

```bash
./your_program.sh          # starts the server on port 6379
```

In another terminal:

```bash
redis-cli PING
redis-cli SET foo bar PX 5000
redis-cli GET foo
```

Run the CodeCrafters tests with:

```bash
codecrafters test
codecrafters submit
```

## Supported commands

| Area | Commands |
|---|---|
| Basics | `PING`, `ECHO` |
| Strings | `SET` (with `EX` / `PX` expiry), `GET`, `INCR` |
| Lists | `RPUSH`, `LPUSH` (multiple elements), `LRANGE` (including negative indexes), `LLEN`, `LPOP` (optional count), `BLPOP` (blocking, with timeout) |
| Streams | `XADD` (explicit IDs, `<ms>-*` and `*` auto-generation, ID validation), `XRANGE` (including `-` and `+`), `XREAD` (multiple streams, `BLOCK`, `BLOCK 0`, `$`) |
| Introspection | `TYPE` (`string`, `list`, `stream`, `none`) |

## Project layout

```
src/main/java/
├── Main.java            Networking only: accepts connections, one virtual thread per client
├── Resp.java            RESP parsing (requests) and encoding (replies)
├── CommandHandler.java  Command logic and the in-memory data store
├── RedisStream.java     A stream: an ordered list of entries
└── StreamId.java        Stream entry ID (<ms>-<seq>) with ordering
```

Each file has one job: `Main` knows about sockets, `Resp` knows the wire format,
and `CommandHandler` knows Redis commands. Adding a command means adding one
`case` in `CommandHandler.handle()` and one small method.

## How it works

**Protocol.** Clients send commands as RESP arrays of bulk strings
(`*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n`). `Resp.readCommand` reads exactly one
command from the socket and returns it as a `List<String>`. Replies are built
with helpers such as `simple`, `bulk`, `integer`, `array`, and `rawArray`
(for nested arrays like `XRANGE` results).

**Concurrency.** Each client connection runs on its own virtual thread, so a
blocked client (`BLPOP`, `XREAD BLOCK`) never holds up anyone else. All clients
share one `CommandHandler`, and therefore one store.

**Storage.** A `ConcurrentHashMap<String, Entry>`, where an `Entry` holds a
value (`String`, `List<String>`, or `RedisStream`) plus an optional expiry
timestamp. Every command that reads, changes, then writes a key does so inside
`ConcurrentHashMap.compute()`, which is atomic per key. That keeps commands like
`INCR`, `LPOP`, and `XADD` correct when many clients hit the same key.

**Expiry.** Lazy: the expiry is stored as an absolute timestamp, and a key is
discarded when a command finds it expired.

**Blocking `BLPOP`.** Each list key has a FIFO queue of waiting clients (a
`CompletableFuture` each). A push hands elements to the longest-waiting client
first. Registering as a waiter happens inside the same `compute()` as the
empty-list check, so a push can't slip in between and leave a client waiting on a
list that already has data.

**Blocking `XREAD`.** A blocked reader registers a wake-up signal on its stream
keys, then reads. `XADD` stores the entry and signals the waiting readers, and
each reader re-reads using its own start IDs. `$` is resolved once to the
stream's last ID before the first read, so it can't drift while waiting.

**Streams.** Entry IDs are parsed into `StreamId(ms, seq)` and compared
numerically. `XADD` requires every new ID to be greater than the last one, and
resolves auto-generated IDs inside `compute()` so concurrent writers get unique,
increasing IDs.

## Known limitations

- A client that disconnects while blocked in `BLPOP` leaves its waiter in the
  queue, so a later push may be handed to a dead connection.
- Expiry is lazy only. Keys nobody reads stay in memory until accessed.
- `LPUSH` and `LPOP` on an `ArrayList` are O(n).
- No persistence (RDB/AOF) and no replication yet.

## Roadmap

- `INCR` edge cases (missing key, non-numeric value)
- Transactions (`MULTI` / `EXEC` / `DISCARD`)
- Replication
- Persistence
- Active expiry (background sweep)