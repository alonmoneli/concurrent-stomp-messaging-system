package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;

public class StompServer {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage is for port, server type - tpc/reactor)");
            return;
        }
        int port = Integer.parseInt(args[0]);
        String serverType = args[1];
        if (serverType.equals("tpc")) {
            Server.threadPerClient(port, () -> new StompMessagingProtocolImpl(), () -> new StompMessageEncoderDecoder()).serve();
        } 
        else if ("reactor".equals(serverType)) {
            Server.reactor(Runtime.getRuntime().availableProcessors(), port, () -> new StompMessagingProtocolImpl(), () -> new StompMessageEncoderDecoder()).serve();
        } 
        else {
            System.out.println("This server type is illegal - " + serverType);
        }
    }
}