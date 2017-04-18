package kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;


public class BananaPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

        int numPartitions = partitions.size();

        if((keyBytes == null) || (!(key instanceof String)))
            throw new InvalidRecordException("Invalid record");


        if(( (String) key).equals("banana")) {
            return numPartitions; // if key is banana, it will be always the last partition
        }

        // else will use hash generation code.
        return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1));

    }
}