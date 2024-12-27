package protocol;

import lombok.Getter;
import lombok.experimental.Accessors;
import message.apiversions.ApiVersionRequestV4;
import protocol.io.DataInput;
import protocol.io.DataOutput;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Getter
@Accessors(fluent = true)
public class ExchangeMapper {

    private final Map<RequestApi, Function<DataInput, ? extends RequestBody>> deserializers = new HashMap<>();

    public ExchangeMapper() {
        deserializers.put(ApiVersionRequestV4.API, ApiVersionRequestV4::deserialize);
    }

    public Request receiveRequest(DataInput input) {
        final var messageSize = input.readSignedInt();
        final var buffer = input.readNBytes(messageSize);


        return new Request(null, null);
    }

    public void sendResponse(DataOutput output, Response response) {
        output.writeInt(5);
    }

}
