package client;

import kafka.Kafka;
import lombok.Getter;
import lombok.SneakyThrows;
import message.apiversions.ApiVersionRequestV4;
import message.apiversions.ApiVersionsResponseV4;
import message.describetopic.DescribeTopicPartitionsRequestV0;
import message.describetopic.DescribeTopicPartitionsResponseV0;
import protocol.*;
import protocol.io.DataInputStream;
import protocol.io.DataOutputStream;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

@Getter
public class Client implements Runnable {

    private final Kafka kafka;
    private final ExchangeMapper mapper;
    private final Socket socket;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;

    public Client(Kafka kafka, ExchangeMapper mapper, Socket socket) throws IOException {
        this.kafka = kafka;
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

            case DescribeTopicPartitionsRequestV0 describeTopicPartitionsRequest -> new Response(
                    new Header.V1(request.header().correlationId()),
                    handleDescribeTopicPartitionsRequest(describeTopicPartitionsRequest)
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


        return new ApiVersionsResponseV4(
                keys,
                Duration.ZERO
        );
    }

    @SneakyThrows
    private DescribeTopicPartitionsResponseV0 handleDescribeTopicPartitionsRequest(DescribeTopicPartitionsRequestV0 request) {
        final var topicResponses = new ArrayList<DescribeTopicPartitionsResponseV0.Topic>();

        for (final var topicRequest : request.topics()) {
            final var topicRecord = kafka.getTopic(topicRequest.name());

            if (topicRecord == null) {
                topicResponses.add(new DescribeTopicPartitionsResponseV0.Topic(
                        ErrorCode.UNKNOWN_TOPIC,
                        topicRequest.name(),
                        UUID.fromString("00000000-0000-0000-0000-000000000000"),
                        false,
                        Collections.emptyList(),
                        0
                ));

                continue;
            }

            final var partitionResponses = new ArrayList<DescribeTopicPartitionsResponseV0.Topic.Partition>();
            for (final var partitionRecord : kafka.getPartitions(topicRecord.id())) {
                partitionResponses.add(new DescribeTopicPartitionsResponseV0.Topic.Partition(
                        ErrorCode.NONE,
                        partitionRecord.id(),
                        partitionRecord.leader(),
                        partitionRecord.leaderEpoch(),
                        partitionRecord.replicas(),
                        partitionRecord.inSyncReplicas(),
                        partitionRecord.addingReplicas(),
                        Collections.emptyList(),
                        partitionRecord.removingReplicas()
                ));
            }

            topicResponses.add(new DescribeTopicPartitionsResponseV0.Topic(
                    ErrorCode.NONE,
                    topicRequest.name(),
                    topicRecord.id(),
                    false,
                    partitionResponses,
                    0
            ));
        }

        return new DescribeTopicPartitionsResponseV0(
                Duration.ZERO,
                topicResponses,
                null
        );
    }
}
