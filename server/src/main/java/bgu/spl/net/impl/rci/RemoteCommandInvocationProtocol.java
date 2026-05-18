package bgu.spl.net.impl.rci;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import java.io.Serializable;

public class RemoteCommandInvocationProtocol<T> implements StompMessagingProtocol<Serializable> {

    private T arg;
    private Connections<Serializable> connections;
    private int connectionId;

    public RemoteCommandInvocationProtocol(T arg) {
        this.arg = arg;
    }

    @Override
    public void start(int connectionId, Connections<Serializable> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(Serializable msg) {
        Serializable response = ((Command) msg).execute(arg);
        if (connections != null) {
            connections.send(connectionId, response);
        }
    }

    @Override
    public boolean shouldTerminate() {
        return false;
    }

}
