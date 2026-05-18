#!/usr/bin/env python3
"""
Basic Python Server for STOMP Assignment – Stage 3.3

IMPORTANT:
DO NOT CHANGE the server name or the basic protocol.
Students should EXTEND this server by implementing
the methods below.
"""

import socket
import sys
import threading
import sqlite3

SERVER_NAME = "STOMP_PYTHON_SQL_SERVER"  # DO NOT CHANGE!
DB_FILE = "stomp_server.db"              # DO NOT CHANGE!

def recv_null_terminated(sock: socket.socket) -> str:
    data = b""
    while True:
        chunk = sock.recv(1024)
        if not chunk:
            return ""
        data += chunk
        if b"\0" in data:
            msg, _ = data.split(b"\0", 1)
            return msg.decode("utf-8", errors="replace")


def init_database():
    try:
        connect = sqlite3.connect(DB_FILE)
        cursor = connect.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        username TEXT PRIMARY KEY, 
                        password TEXT,
                        registration_date TEXT)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS login_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT, 
                        login_time TEXT, 
                        logout_time TEXT,
                        FOREIGN KEY(username) REFERENCES users(username))''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS file_tracking (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT, 
                        filename TEXT, 
                        upload_time TEXT,
                        game_channel TEXT,
                        FOREIGN KEY(username) REFERENCES users(username))''')
        connect.commit()
        connect.close()
        print(f"[{SERVER_NAME}] Database initialized.")
    except Exception as e:
        print(f"[{SERVER_NAME}] DB Init Error: {e}")


def execute_sql_command(sql_command: str) -> str:
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(sql_command)
        conn.commit()
        conn.close()
        return "SUCCESS" 
    except Exception as e:
        return f"ERROR: {e}"


def execute_sql_query(sql_query: str) -> str:
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(sql_query)
        rows = c.fetchall()
        conn.close()
        response = "SUCCESS"
        for row in rows:
            response += "|" + str(row)
        return response
    except Exception as e:
        return f"ERROR: {e}"


def handle_client(client_socket: socket.socket, addr):
    print(f"[{SERVER_NAME}] Client connected from {addr}")

    try:
        while True:
            message = recv_null_terminated(client_socket)
            if message == "":
                break

            print(f"[{SERVER_NAME}] Received: {message}")
            command = message.strip()
            response = ""
            if command.upper().startswith("SELECT"):
                response = execute_sql_query(command)
            else:
                response = execute_sql_command(command)
            client_socket.sendall(response.encode('utf-8') + b"\0")

    except Exception as e:
        print(f"[{SERVER_NAME}] Error handling client {addr}: {e}")
    finally:
        try:
            client_socket.close()
        except Exception:
            pass
        print(f"[{SERVER_NAME}] Client {addr} disconnected")


def start_server(host="127.0.0.1", port=7778):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"[{SERVER_NAME}] Server started on {host}:{port}")
        print(f"[{SERVER_NAME}] Waiting for connections...")

        while True:
            client_socket, addr = server_socket.accept()
            t = threading.Thread(
                target=handle_client,
                args=(client_socket, addr),
                daemon=True
            )
            t.start()

    except KeyboardInterrupt:
        print(f"\n[{SERVER_NAME}] Shutting down server...")
    finally:
        try:
            server_socket.close()
        except Exception:
            pass


if __name__ == "__main__":
    port = 7778
    init_database()
    if len(sys.argv) > 1:
        raw_port = sys.argv[1].strip()
        try:
            port = int(raw_port)
        except ValueError:
            print(f"Invalid port '{raw_port}', falling back to default {port}")
    start_server(port=port)