package client;

import lombok.Getter;
import lombok.SneakyThrows;
import message.apiversions.ApiVersionRequestV4;
import message.apiversions.ApiVersionsResponseV4;
import protocol.*;
import protocol.io.DataInputStream;
import protocol.io.DataOutputStream;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;

@Getter
public class Client implements Runnable {

    private final ExchangeMapper mapper;
    private final Socket socket;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;

    public Client(ExchangeMapper mapper, Socket socket) throws IOException {
        this.mapper = mapper;
        this.socket = socket;

        this.inputStream = new DataInputStream(socket.getInputStream());
        this.outputStream = new DataOutputStream(socket.getOutputStream());
    }

    @SneakyThrows
    @Override
    public void run() {
        try (socket) {
            while (!socket.isClosed()) {
                exchange();
            }
        } catch (Exception exception) {
            System.err.println("%s: %s".formatted(socket.getLocalSocketAddress(), exception.getMessage()));

            if (!(exception instanceof EOFException)) {
                exception.printStackTrace();
            }
        }
    }

    private void exchange() {
        try {
            final var request = mapper.receiveRequest(inputStream);
            final var correlationId = request.header().correlationId();

            final var response = handle(request);
            if (response == null) throw new ProtocolException(ErrorCode.UNKNOWN_SERVER_ERROR, correlationId);

            mapper.sendResponse(outputStream, response);
        } catch (ProtocolException exception) {
            mapper.sendErrorResponse(outputStream, exception.correlationId(), exception.code());
        }
    }

    private Response handle(Request request) {
        return switch (request.body()) {
            case ApiVersionRequestV4 apiVersionRequest -> new Response(
                    new Header.V0(request.header().correlationId()),
                    handleApiVersionsRequest(apiVersionRequest)
            );

            default -> null;
        };
    }

    private ApiVersionsResponseV4 handleApiVersionsRequest(ApiVersionRequestV4 request) {
        final var keys = mapper.requestApis()
                .stream()
                .map((requestApi) -> new ApiVersionsResponseV4.Key(
                        requestApi.key(),
                        requestApi.version(),
                        requestApi.version())
                )
                .toList();

        System.out.println(keys);

        return new ApiVersionsResponseV4(
                keys,
                Duration.ZERO
        );
    }

}
