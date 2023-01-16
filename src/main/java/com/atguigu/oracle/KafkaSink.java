package com.atguigu.oracle;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 发送到kafka
 * @author gym
 * @version v1.0
 * @description:
 * @date: 2022/4/1 15:57
 */
public class KafkaSink extends RichSinkFunction<Tuple2<String, String>> {

    Producer producer = null;

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        producer.send(new ProducerRecord(value.f0, value.f1));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        producer = createProducer();
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }


    /**
     * 创建Producer实例
     */
    public Producer<String, String> createProducer() {
        Properties properties = new Properties();
        //配置文件里面的变量都是静态final类型的，并且都有默认的值
        //用于建立与 kafka 集群连接的 host/port
        //继承的hashtable,保证了线程安全
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LocalFileConfigParam.getPropertiesString("kafka.bootstrap.servers", "localhost:9092"));
        /**
         * producer 需要 server 接收到数据之后发出的确认接收的信号，此项配置就是指 procuder需要多少个这样的确认信号。此配置实际上代表
         * 了数据备份的可用性。以下设置为常用选项：
         * （1）acks=0： 设置为 0 表示 producer 不需要等待任何确认收到的信息。副本将立即加到socket buffer 并认为已经发送。没有任何保
         * 障可以保证此种情况下 server 已经成功接收数据，同时重试配置不会发生作用（因为客户端不知道是否失败）回馈的 offset 会总是设置为-1；
         * （2）acks=1： 这意味着至少要等待 leader已经成功将数据写入本地 log，但是并没有等待所有 follower 是否成功写入。这种情况下，如
         * 果 follower 没有成功备份数据，而此时 leader又挂掉，则消息会丢失。
         * （3）acks=all： 这意味着 leader 需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
         * （4）其他的设置，例如 acks=2 也是可以的，这将需要给定的 acks 数量，但是这种策略一般很少用
         **/
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        /**
         设置大于 0 的值将使客户端重新发送任何数据，一旦这些数据发送失败。注意，这些重试与客户端接收到发送错误时的重试没有什么不同。允许
         重试将潜在的改变数据的顺序，如果这两个消息记录都是发送到同一个 partition，则第一个消息失败第二个发送成功，则第二条消息会比第一
         条消息出现要早
         **/
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        /**
         * producer 将试图批处理消息记录，以减少请求次数。这将改善 client 与 server 之间的性能。这项配置控制默认的批量处理消息字节数。
         * 不会试图处理大于这个字节数的消息字节数。发送到 brokers 的请求将包含多个批量处理，其中会包含对每个 partition 的一个请求。
         * 较小的批量处理数值比较少用，并且可能降低吞吐量（0 则会仅用批量处理）。较大的批量处理数值将会浪费更多内存空间，这样就需要分配特
         * 定批量处理数值的内存大小
         **/
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        /**
         * producer 组将会汇总任何在请求与发送之间到达的消息记录一个单独批量的请求。通常来说，这只有在记录产生速度大于发送速度的时候才
         * 能发生。然而，在某些条件下，客户端将希望降低请求的数量，甚至降低到中等负载一下。这项设置将通过增加小的延迟来完成--即，不是立即
         * 发送一条记录，producer 将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理。这可以认为是 TCP 种 Nagle 的算
         * 法类似。这项设置设定了批量处理的更高的延迟边界：一旦我们获得某个 partition 的batch.size，他将会立即发送而不顾这项设置，
         * 然而如果我们获得消息字节数比这项设置要小的多，我们需要“linger”特定的时间以获取更多的消息。 这个设置默认为 0，即没有延迟。设
         * 定 linger.ms=5，例如，将会减少请求数目，但是同时会增加 5ms 的延迟
         **/
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        /**
         * producer 可以用来缓存数据的内存大小。如果数据产生速度大于向 broker 发送的速度，将会耗尽这个缓存空间,producer
         * 会阻塞或者抛出异常，以“block.on.buffer.full”来表明。这项设置将和 producer 能够使用的总内存相关，但并不是一个
         * 硬性的限制，因为不是producer 使用的所有内存都是用于缓存。一些额外的内存会用于压缩（如果引入压缩机制），同样还有一些
         * 用于维护请求当缓存空间耗尽，其他发送调用将被阻塞，阻塞时间的阈值通过max.block.ms设定，之后它将抛出一个TimeoutException。
         **/
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        /**
         * 该配置控制 KafkaProducer's send(),partitionsFor(),inittransaction (),sendOffsetsToTransaction(),commitTransaction() "
         * 和abortTransaction()方法将阻塞。对于send()，此超时限制了获取元数据和分配缓冲区的总等待时间"
         **/
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");

        //将消息发送到kafka server, 所以肯定需要用到序列化的操作  我们这里发送的消息是string类型的，所以使用string的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(properties);
    }
}

