# Concurrent STOMP Messaging System

A client-server messaging system with a Java server, a multithreaded C++ client, and a Python/SQLite service for recording user activity.

The application uses a subset of STOMP to publish and receive sports-event reports through shared channels. Clients can subscribe to games, report events from JSON files, and generate local summaries.

## Features

- Java server with two concurrency models:
  - Thread-per-client using blocking sockets.
  - Reactor using Java NIO and a worker-thread pool.
- C++ client with separate keyboard-input and network-reading threads.
- Channel subscriptions and publish/subscribe messaging.
- STOMP commands: CONNECT, SUBSCRIBE, UNSUBSCRIBE, SEND, and DISCONNECT.
- CONNECTED, MESSAGE, RECEIPT, and ERROR responses.
- JSON event-file parsing and local game-summary generation.
- Python/SQLite service for recording registrations and login/logout activity.
- Included Linux development-container configuration.

## Architecture

| Component | Responsibility |
| --- | --- |
| Java server | Connections, subscriptions, message routing, and protocol handling |
| C++ client | User commands, event reporting, incoming messages, and summaries |
| Python SQL service | Executes database requests received from the Java server |
| SQLite database | Stores recorded user and session activity |

The Java server connects to the SQL service at `127.0.0.1:7778`.

Authentication state is maintained in memory by the Java server. The SQLite database records activity; it does not currently restore authentication state after a server restart.

## Requirements

- Linux or WSL
- JDK 8 or newer
- Maven
- G++ with C++17 support
- GNU Make
- Boost development libraries
- Python 3 with SQLite support

On Ubuntu or Debian:

```bash
sudo apt-get update
sudo apt-get install build-essential libboost-all-dev default-jdk maven python3
```

## Build

Run from the repository root:

```bash
mvn -f server/pom.xml clean compile
mkdir -p client/bin
make -C client
```

The STOMP client executable is created at `client/bin/StompWCIClient`.

## Run

Use separate terminals for the database service, Java server, and client.

### 1. Start the database service

From the repository root:

```bash
cd data
python3 sql_server.py
```

The service listens on port `7778` and creates `stomp_server.db` if needed.

### 2. Start the Java server

From the repository root, choose one mode.

Thread-per-client:

```bash
java -cp server/target/classes bgu.spl.net.impl.stomp.StompServer 7777 tpc
```

Reactor:

```bash
java -cp server/target/classes bgu.spl.net.impl.stomp.StompServer 7777 reactor
```

### 3. Start the C++ client

From the repository root:

```bash
cd client
./bin/StompWCIClient
```

## Example Session

Enter the following commands in the client, waiting for login confirmation before joining the channel:

```text
login 127.0.0.1:7777 alice demo-password
join Germany_Japan
report data/events1.json
summary Germany_Japan alice summary.txt
exit Germany_Japan
logout
```

Wait for the event messages to arrive before generating the summary.

To demonstrate messaging between clients, launch another client, log in with a different username, and join `Germany_Japan` before the first client reports the events.

## Client Commands

| Command | Purpose |
| --- | --- |
| `login <host:port> <username> <password>` | Connect and log in |
| `join <game>` | Subscribe to a game channel |
| `exit <game>` | Unsubscribe from a channel |
| `report <file>` | Publish events from a JSON file |
| `summary <game> <user> <file>` | Write a local summary of received reports |
| `logout` | Request disconnection |

## Repository Layout

- `server/` — Java networking framework and STOMP implementation.
- `client/` — C++ client, Makefile, and sample event files.
- `data/` — Python SQL service.
- `.devcontainer/` — Linux development-container configuration.

## Scope

This is an educational implementation of a subset of STOMP, intended for local demonstrations and concurrency/networking practice.

It does not implement the complete STOMP specification. Credentials are currently stored as plaintext, and encrypted transport is not configured. Use demonstration credentials only.
