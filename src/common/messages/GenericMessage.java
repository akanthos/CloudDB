package common.messages;

import java.io.UnsupportedEncodingException;

/**
 * Created by akanthos on 11.11.15.
 */
public interface GenericMessage {
    public byte[] getMsgBytes() throws UnsupportedEncodingException;
}
