package kafka;


import kafka.record.Record;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Kafka {

    private final String logsRoot;
    private final Map<UUID, Record.Topic> topicPerId;
    private final Map<String, Record.Topic> topicPerName;
    private final Map<UUID, List<Record.Partition>> partitionsPerTopicId;

    public Kafka(String logsRoot, List<Record.Topic> topics, List<Record.Partition> records) {
        this.logsRoot = logsRoot;

        this.topicPerId = topics
                .stream()
                .collect(Collectors.toMap(
                        Record.Topic::id,
                        Function.identity()
                ));

        this.topicPerName = topics
                .stream()
                .collect(Collectors.toMap(
                        Record.Topic::name,
                        Function.identity()
                ));

        this.partitionsPerTopicId = records
                .stream()
                .sorted(Comparator.comparing(Record.Partition::topicId))
                .collect(Collectors.groupingBy(
                        Record.Partition::topicId
                ));
    }

    public Record.Topic getTopic(UUID id) {
        return topicPerId.get(id);
    }
}
