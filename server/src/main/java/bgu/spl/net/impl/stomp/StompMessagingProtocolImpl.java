package bgu.spl.net.impl.stomp;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.data.Database;
import bgu.spl.net.impl.data.LoginStatus;
import bgu.spl.net.srv.Connections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger(1);
    private static final ConcurrentMap<String, ConcurrentMap<Integer, String>> DESTINATIONS = new ConcurrentHashMap<>();
    private final Database database = Database.getInstance();
    private Connections<String> connections;
    private int connectionID;
    private boolean shouldTerminate;
    private final Map<String, String> subsById = new ConcurrentHashMap<>();
    private boolean loggedIn;
    private String username;

    @Override
    public void start(int connectionID, Connections<String> connections) {
        this.connectionID = connectionID;
        this.connections = connections;
    }

    @Override
    public void process(String message) {
        Frame frame = Frame.parse(message);
        if (frame == null || frame.command.isEmpty()) {
            sendError("illegal frame", frame, message);
            return;
        }
        switch (frame.command) {
            case "SEND":
                handleSend(frame, message);
                break;
            case "CONNECT":
                handleConnect(frame, message);
                break;
            case "DISCONNECT":
                handleDisconnect(frame);
                break;
            case "SUBSCRIBE":
                handleSubscribe(frame, message);
                break;
            case "UNSUBSCRIBE":
                handleUnsubscribe(frame, message);
                break;
            default:
                sendError("illegal input", frame, message);
        }
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    private void handleConnect(Frame frame, String rawMessage) {
        if (loggedIn) {
            sendError("User already logged in", frame, rawMessage);
            return;
        }
        String login = frame.headers.get("login");
        String passcode = frame.headers.get("passcode");
        if (login == null || passcode == null) {
            sendError("missing login or passcode", frame, rawMessage);
            return;
        }
        LoginStatus status = database.login(connectionID, login, passcode);
        switch (status) {
            case ADDED_NEW_USER: //same behavior - remember if asked' because you may not.
            case LOGGED_IN_SUCCESSFULLY:
                loggedIn = true;
                username = login;
                sendToClient(buildConnectedFrame());
                sendReceiptIfRequested(frame);
                break;
            case CLIENT_ALREADY_CONNECTED:
                sendError("Client already connected", frame, rawMessage);
                break;
            case ALREADY_LOGGED_IN:
                sendError("User already logged in", frame, rawMessage);
                break;
            case WRONG_PASSWORD:
                sendError("Wrong Password", frame, rawMessage);
                break;
            default:
                sendError("login failed", frame, rawMessage);
        }
    }

    private void handleSend(Frame frame, String rawMessage) {
        ensureLoggedIn(frame, rawMessage);
        String destination = frame.headers.get("destination");
        if (destination == null) {
            sendError("missing destination", frame, rawMessage);
            return;
        }
        if (!isSubscribedTo(destination)) {
            sendError("User not subscribed to destination", frame, rawMessage);
            return;
        }
        ConcurrentMap<Integer, String> subscribers = DESTINATIONS.get(destination);
        if (subscribers != null) {
            for (Map.Entry<Integer, String> entry : subscribers.entrySet()) {
                String subscriptionId = entry.getValue();
                int subscriberid = entry.getKey();
                String msg = buildMessageFrame(destination, subscriptionId, frame.body);
                connections.send(subscriberid, msg);
            }
        }
        sendReceiptIfRequested(frame);
    }

    private void handleSubscribe(Frame frame, String rawMessage) {
        ensureLoggedIn(frame, rawMessage);
        String destination = frame.headers.get("destination");
        String id = frame.headers.get("id");
        if (destination == null || id == null) {
            sendError("missing destination/id", frame, rawMessage);
            return;
        }
        if (subsById.containsKey(id)) {
            sendError("subscription id already used", frame, rawMessage);
            return;
        }
        subsById.put(id, destination);
        DESTINATIONS.computeIfAbsent(destination, key -> new ConcurrentHashMap<>()).put(connectionID, id);
        sendReceiptIfRequested(frame);
    }

    private void handleUnsubscribe(Frame frame, String rawMessage) {
        ensureLoggedIn(frame, rawMessage);
        String id = frame.headers.get("id");
        if (id == null) {
            sendError("missing id", frame, rawMessage);
            return;
        }
        String destination = subsById.remove(id);
        if (destination == null) {
            sendError("unknown subscription id", frame, rawMessage);
            return;
        }

        ConcurrentMap<Integer, String> subscribers = DESTINATIONS.get(destination);
        if (subscribers != null) {
            subscribers.remove(connectionID);
            if (subscribers.isEmpty()) {
                DESTINATIONS.remove(destination, subscribers);
            }
        }
        sendReceiptIfRequested(frame);
    }

    private void handleDisconnect(Frame frame) {
        sendReceiptIfRequested(frame);
        disconnectClient();
    }

    private void ensureLoggedIn(Frame frame, String rawMessage) {
        if (!loggedIn) {
            sendError("Not logged in", frame, rawMessage);
        }
    }

    private void disconnectClient() {
        if (loggedIn) {
            database.logout(connectionID);
        }
        for (String destination : subsById.values()) {
            ConcurrentMap<Integer, String> subscribers = DESTINATIONS.get(destination);
            if (subscribers != null) {
                subscribers.remove(connectionID);
            }
        }
        subsById.clear();
        if (connections != null) {
            connections.disconnect(connectionID);
        }
        shouldTerminate = true;
    }

    private boolean isSubscribedTo(String destination) {
        return subsById.containsValue(destination);
    }

    private void sendReceiptIfRequested(Frame frame) {
        String receipt = frame.headers.get("receipt");
        if (receipt != null) {
            sendToClient(buildReceiptFrame(receipt));
        }
    }

    private void sendError(String message, Frame frame, String originalMessage) {
        String receipt = frame != null ? frame.headers.get("receipt") : null;
        String errorFrame = buildErrorFrame(message, receipt, originalMessage);
        sendToClient(errorFrame);
        disconnectClient();
    }

    private void sendToClient(String frame) {
        if (connections != null) {
            connections.send(connectionID, frame);
        }
    }

    private static String buildConnectedFrame() {
        return "CONNECTED\nversion:1.2\n\n";
    }

    private static String buildReceiptFrame(String receiptId) {
        return "RECEIPT\nreceipt-id:" + receiptId + "\n\n";
    }

    private static String buildMessageFrame(String destination, String subscriptionId, String body) {
        int messageId = MESSAGE_ID.getAndIncrement();
        StringBuilder sb = new StringBuilder();
        sb.append("MESSAGE\n");
        sb.append("subscription:").append(subscriptionId).append('\n');
        sb.append("message-id:").append(messageId).append('\n');
        sb.append("destination:").append(destination).append('\n');
        sb.append('\n');
        if (body != null) {
            sb.append(body);
        }
        return sb.toString();
    }

    private static String buildErrorFrame(String message, String receiptId, String originalMessage) {
        StringBuilder sb = new StringBuilder();
        sb.append("ERROR\n");
        if (receiptId != null) {
            sb.append("receipt-id:").append(receiptId).append('\n');
        }
        sb.append("message:").append(message).append('\n');
        sb.append('\n');
        if (originalMessage != null) {
            sb.append("The message:\n-----\n");
            sb.append(originalMessage);
            sb.append("\n-----\n");
        }
        return sb.toString();
    }

    private static class Frame {
        private final String command;
        private final Map<String, String> headers;
        private final String body;

        private Frame(String command, Map<String, String> headers, String body) {
            this.command = command;
            this.headers = headers;
            this.body = body;
        }

        private static Frame parse(String message) {
            if (message == null) {
                return null;
            }
            String[] lines = message.split("\n", -1);
            if (lines.length == 0) {
                return null;
            }
            String command = lines[0].trim();
            Map<String, String> headers = new ConcurrentHashMap<>();
            int i = 1;
            for (; i < lines.length; i++) {
                String line = lines[i];
                if (line.isEmpty()) {
                    i++;
                    break;
                }
                int colonIndex = line.indexOf(':');
                if (colonIndex <= 0) {
                    continue;
                }
                String key = line.substring(0, colonIndex).trim();
                String value = line.substring(colonIndex + 1).trim();
                headers.put(key, value);
            }
            StringBuilder body = new StringBuilder();
            for (; i < lines.length; i++) {
                body.append(lines[i]);
                if (i < lines.length - 1) {
                    body.append('\n');
                }
            }
            return new Frame(command, headers, body.toString());
        }
    }
}
