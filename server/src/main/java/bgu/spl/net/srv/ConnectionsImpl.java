package bgu.spl.net.srv;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConnectionsImpl<T> implements Connections<T> {
    private final ConcurrentMap<Integer, ConnectionHandler<T>> handlers = new ConcurrentHashMap<>();

    public void addConnection(int connectionId, ConnectionHandler<T> handler) {
        handlers.put(connectionId, handler);
    }

    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = handlers.get(connectionId);
        if (handler == null) {
            return false;
        }
        handler.send(msg);
        return true;
    }

    @Override
    public void send(String channel, T msg) {
        for (ConnectionHandler<T> handler : handlers.values()) {
            handler.send(msg);
        }
    }

    @Override
    public void disconnect(int connectionId) {
        ConnectionHandler<T> handler = handlers.remove(connectionId);
        if (handler != null) {
            if (handler instanceof NonBlockingConnectionHandler) {
                return;
            }
            try {
                handler.close();
            } catch (IOException ignored) {
                // We will ignore close errors as we do not need to give them attention.
            }
        }
    }
}
