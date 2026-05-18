# STOMP Client-Server Network Architecture

An end-to-end, high-performance client-server system implementing a custom subset of the **STOMP (Streaming Text Oriented Messaging Protocol)**. This project demonstrates advanced network programming, multi-threaded concurrency models, and clean architectural design patterns developed and tested within a isolated Linux container environment.

## 🚀 Key Features & Architecture
* **Multi-Threaded Server Frameworks:** Features two distinct concurrency patterns in Java:
  * **Thread-Per-Client (TPC):** A blocking I/O model managing dedicated execution threads per active connection.
  * **Reactor Pattern:** A non-blocking, event-driven architecture using single-thread multiplexing to efficiently scale across hundreds of concurrent clients.
* **Robust C++ Client:** A modern, Object-Oriented C++ client implementing synchronized network split-loops (isolated reading and writing threads) to communicate smoothly with the message broker.
* **Custom Protocol Parser:** Implements strict serialization, frame validation, and state-machine tracking for core STOMP commands (`CONNECT`, `SUBSCRIBE`, `SEND`, `DISCONNECT`).
* **Containerized R&D Environment:** Fully built, compiled, and debugged inside a dedicated **Linux Dev Container (Docker)**, ensuring environment parity and clean dependency management.

## 🛠️ Tech Stack
* **Languages:** Modern C++, Java
* **Networking & Concurrency:** Sockets, Non-blocking I/O (NIO), Thread Pooling, Synchronization Primitives
* **Environments & Tools:** Linux, Docker (Dev Containers), Git, GNU Make, Maven

## 📦 Environment Setup & Build
Since the system is designed around standard POSIX and Java networking libraries, it is built to run seamlessly inside Linux-based environments.

### Prerequisites
* GCC/G++ Compiler with C++11 support (or higher)
* Java Development Kit (JDK 8+)
* Maven & GNU Make

### Building the Project
To compile both the server and client components, execute the following commands in your Linux terminal:

```bash
# Build the Java Server infrastructure
mvn compile

# Compile the C++ Client executable
make
