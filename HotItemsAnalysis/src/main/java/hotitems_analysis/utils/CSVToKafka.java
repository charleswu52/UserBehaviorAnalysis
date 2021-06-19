package hotitems_analysis.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.io.BufferedSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author WuChao
 * @create 2021/6/19 17:08
 */
public class CSVToKafka {
    /**
     * 使用 Flink 向 Kafka中 传数据
     */
    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topic = "hotitems";
        File file = new File("HotItemsAnalysis/src/main/resources/UserBehavior.csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String data = null;
        while ((data = reader.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, data);
            System.out.println("Sending msg:" + data);
            producer.send(record);
        }
        producer.close();
        reader.close();
    }
}
