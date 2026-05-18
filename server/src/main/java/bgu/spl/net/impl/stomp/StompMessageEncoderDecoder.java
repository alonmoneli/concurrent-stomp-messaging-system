package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class StompMessageEncoderDecoder implements MessageEncoderDecoder<String> {
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    @Override
    public String decodeNextByte(byte nextByte) {
        if (nextByte == '\u0000') {
            String frame = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
            buffer.reset();
            return frame;
        }
        buffer.write(nextByte);
        return null;
    }

    @Override
    public byte[] encode(String message) {
        return (message + '\u0000').getBytes(StandardCharsets.UTF_8);
    }
}
