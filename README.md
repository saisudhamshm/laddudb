
# Redis-like Database Implementation in Go

A high-performance, Redis-compatible database implementation in Go with support for strings, lists, streams, transactions, and blocking operations.

## Features

### Data Types Support

- **Strings**: Basic key-value storage with expiration support (TTL)
- **Lists**: Doubly-ended lists with push/pop operations from both ends
- **Streams**: Append-only log data structure with time-ordered entries and blocking reads


### Core Functionality

- **Transactions**: MULTI/EXEC/DISCARD support for atomic operations
- **Blocking Operations**: XREAD with BLOCK support for real-time stream processing
- **Expiration**: TTL support for keys with automatic cleanup
- **Persistence**: RDB file format support for data durability
- **Replication**: Master-slave replication with command propagation
- **Type Safety**: Proper type checking with Redis-compatible error messages


### Redis Protocol Compatibility

- RESP (Redis Serialization Protocol) support
- Compatible with standard Redis clients
- Familiar Redis command syntax and behavior




## Supported Commands

### String Commands

| Command | Description | Example |
| :-- | :-- | :-- |
| `SET key value` | Set a key to hold a string value | `SET mykey "hello"` |
| `SET key value PX milliseconds` | Set with expiration | `SET temp "value" PX 5000` |
| `GET key` | Get the value of a key | `GET mykey` |
| `INCR key` | Increment the integer value of a key | `INCR counter` |
| `DELTETE key`| Delete a value from the database if exists irrespective of the type| `DELETE mykey`|

### List Commands

| Command | Description | Example |
| :-- | :-- | :-- |
| `LPUSH key element [element ...]` | Push elements to the head of a list | `LPUSH mylist "world" "hello"` |
| `RPUSH key element [element ...]` | Push elements to the tail of a list | `RPUSH mylist "item1" "item2"` |
| `LPOP key` | Remove and return the first element | `LPOP mylist` |
| `RPOP key` | Remove and return the last element | `RPOP mylist` |
| `LLEN key` | Get the length of a list | `LLEN mylist` |
| `LRANGE key start stop` | Get elements in a range | `LRANGE mylist 0 -1` |
| `LINDEX key index` | Get element by index | `LINDEX mylist 0` |

### Stream Commands

| Command | Description | Example |
| :-- | :-- | :-- |
| `XADD key ID field value [field value ...]` | Add entry to stream | `XADD mystream * name "John" age "30"` |
| `XRANGE key start end [COUNT count]` | Read range of entries | `XRANGE mystream - +` |
| `XLEN key` | Get stream length | `XLEN mystream` |
| `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` | Read from streams (with blocking support) | `XREAD BLOCK 1000 STREAMS mystream $` |

### Transaction Commands

| Command | Description | Example |
| :-- | :-- | :-- |
| `MULTI` | Start a transaction | `MULTI` |
| `EXEC` | Execute all queued commands | `EXEC` |
| `DISCARD` | Discard all queued commands | `DISCARD` |

### Utility Commands

| Command | Description | Example |
| :-- | :-- | :-- |
| `PING` | Test connection | `PING` |
| `ECHO message` | Echo the given string | `ECHO "hello world"` |
| `KEYS pattern` | Find all keys matching pattern | `KEYS *` |
| `CONFIG GET parameter` | Get configuration parameter | `CONFIG GET dir` |
| `CONFIG SET parameter value` | Set configuration parameter | `CONFIG SET dir "/tmp"` |
| `INFO` | Get server information | `INFO` |
| `SAVE` | Save the data to disk | `SAVE` |

## Usage Examples

### Basic String Operations

```bash
# Set and get values
SET user:1:name "John Doe"
GET user:1:name
# Returns: "John Doe"

# Set with expiration (5 seconds)
SET session:abc123 "user_data" PX 5000
GET session:abc123
# Returns: "user_data" (if within 5 seconds)

# Increment counters
SET counter 10
INCR counter
# Returns: 11
```


### List Operations

```bash
# Create and manipulate lists
LPUSH shopping_list "milk" "bread"
RPUSH shopping_list "eggs" "butter"
LRANGE shopping_list 0 -1
# Returns: 1) "bread" 2) "milk" 3) "eggs" 4) "butter"

# Get list length
LLEN shopping_list
# Returns: 4

# Pop elements
LPOP shopping_list
# Returns: "bread"

RPOP shopping_list
# Returns: "butter"
```


### Stream Operations

```bash
# Add entries to a stream
XADD events * action "login" user "john" timestamp "2024-01-01T10:00:00Z"
# Returns: "1704100800000-0"

XADD events * action "purchase" user "jane" item "book" price "15.99"
# Returns: "1704100800001-0"

# Read all entries
XRANGE events - +
# Returns: Stream entries with IDs and field-value pairs

# Blocking read (wait for new entries)
XREAD BLOCK 5000 STREAMS events $
# Blocks for 5 seconds waiting for new entries

# Read with count limit
XREAD COUNT 1 STREAMS events 0
# Returns: First entry only

# Get stream length
XLEN events
# Returns: 2
```


### Transaction Example

```bash
# Atomic operations
MULTI
SET account:1:balance 100
SET account:2:balance 50
LPUSH transactions "transfer:1->2:25"
XADD audit_log * action "transfer" amount "25"
EXEC
# Returns: 1) OK 2) OK 3) (integer) 1 4) "1704100800000-0"

# All commands executed atomically
```


### Blocking Operations

```bash
# Terminal 1: Wait for new stream entries
XREAD BLOCK 0 STREAMS notifications $

# Terminal 2: Add a new entry
XADD notifications * type "alert" message "System maintenance"

# Terminal 1 immediately receives the new entry
```


### Error Handling

```bash
# Type mismatch errors
SET mykey "hello"
LPUSH mykey "world"
# Returns: WRONGTYPE Operation against a key holding the wrong kind of value

XADD mykey * field "value"
# Returns: WRONGTYPE Operation against a key holding the wrong kind of value

# Wrong argument count
SET mykey
# Returns: ERR wrong number of arguments for 'set' command

# Unknown command
INVALID_COMMAND
# Returns: ERR unknown command 'INVALID_COMMAND'
```


## Configuration

### Database Settings

```bash
# Set data directory
CONFIG SET dir "/var/lib/redis"

# Set RDB filename
CONFIG SET dbfilename "dump.rdb"

# Get current configuration
CONFIG GET dir
CONFIG GET dbfilename
```


### Persistence

The database supports RDB format for persistence:

```bash
# Manual save
SAVE

# The database saves the data periodically every one minute even without manual intervention

# Data is automatically loaded on startup from RDB file
```



### Transaction Support

- Commands are queued during `MULTI` mode
- `EXEC` executes all commands atomically
- `DISCARD` cancels the transaction
- Proper error handling for nested transactions


### Blocking Operations

- **XREAD BLOCK**: Efficiently blocks waiting for new stream entries
- **Goroutine-based**: Uses Go channels for notification
- **Timeout Support**: Configurable block timeout
- **Multiple Streams**: Can wait on multiple streams simultaneously


### Type Safety

- Each key has an associated data type
- Commands validate data types before execution
- Redis-compatible error messages for type mismatches


## Performance Considerations

- **Memory**: All data is stored in memory for fast access
- **Persistence**: RDB snapshots provide durability
- **Concurrency**: Mutex-based locking ensures thread safety
- **Replication**: Asynchronous command propagation to replicas
- **Blocking Operations**: Efficient notification system using Go channels


## Stream Features

### Stream IDs

- Automatic ID generation using timestamp-sequence format
- User-specified IDs supported
- Monotonically increasing IDs enforced


### Blocking Reads

- `XREAD BLOCK` with timeout support
- Real-time notifications when new entries arrive
- Multiple clients can block on the same stream


### Range Queries

- `XRANGE` for reading historical data
- Support for start/end boundaries
- COUNT option for limiting results




## Building and Running
```bash
./run.sh
```

## Dependencies

- `github.com/hdt3213/rdb` - For RDB file format support
- Standard Go libraries for networking and concurrency
