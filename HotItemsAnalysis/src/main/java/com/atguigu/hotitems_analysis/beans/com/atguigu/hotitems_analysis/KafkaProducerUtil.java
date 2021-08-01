package com.atguigu.hotitems_analysis.beans.com.atguigu.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @Author LiuYang
 * @Date 2021/8/1 7:43 下午
 * 批量的写入kafka脚本
 */
public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");
    }

    // 包装一个写入kafka的方法
    private static void writeToKafka(String topic) throws IOException {
        // kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 用缓冲方式读取文本
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/liuyang/IdeaProjects/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv"));
        String line;

        while ((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            // 用producer发送数据
            producer.send(producerRecord);
        }
        producer.close();
    }
}
