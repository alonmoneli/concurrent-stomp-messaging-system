package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseServer<T> implements Server<T> {

    private final int port;
    private final Supplier<StompMessagingProtocol<T>> protocolFactory;
    private final Supplier<MessageEncoderDecoder<T>> encdecFactory;
    private ServerSocket sock;
    private final ConnectionsImpl<T> connections = new ConnectionsImpl<>();
    private final AtomicInteger nextConnectionId = new AtomicInteger(0);

    public BaseServer(
            int port,
            Supplier<StompMessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> encdecFactory) {

        this.port = port;
        this.protocolFactory = protocolFactory;
        this.encdecFactory = encdecFactory;
		this.sock = null;
    }

    @Override
    public void serve() {

        try (ServerSocket serverSock = new ServerSocket(port)) {
			System.out.println("Server started");

            this.sock = serverSock; //just to be able to close

            while (!Thread.currentThread().isInterrupted()) {
                
                Socket clientSock = serverSock.accept();
                int connectionId = nextConnectionId.getAndIncrement();
                StompMessagingProtocol<T> protocol = protocolFactory.get();
                protocol.start(connectionId, connections);

                BlockingConnectionHandler<T> handler = new BlockingConnectionHandler<>(clientSock, encdecFactory.get(), protocol);
                connections.addConnection(connectionId, handler);

                execute(handler);
            }
        } catch (IOException ex) {
        }

        System.out.println("server closed!!!");
    }

    @Override
    public void close() throws IOException {
		if (sock != null)
			sock.close();
    }

    protected abstract void execute(BlockingConnectionHandler<T>  handler);

}
