package message.fetch;

import protocol.RequestApi;
import protocol.RequestBody;
import protocol.io.DataInput;

import java.time.Duration;
import java.util.List;

public record FetchRequestV16(
        Duration maxWait,
        int minBytes,
        int maxBytes,
        byte isolationLevel,
        int sessionId,
        int sessionEpoch
) implements RequestBody {

    public static final RequestApi API = RequestApi.of(1, 16);

    public static FetchRequestV16 deserialize(DataInput input) {
        final var maxWait = Duration.ofMillis(input.readSignedInt());
        final var minBytes = input.readSignedInt();
        final var maxBytes = input.readSignedInt();
        final var isolationLevel = input.readSignedByte();
        final var sessionId = input.readSignedInt();
        final var sessionEpoch = input.readSignedInt();

        return new FetchRequestV16(
                maxWait,
                minBytes,
                maxBytes,
                isolationLevel,
                sessionId,
                sessionEpoch
        );
    }
}
