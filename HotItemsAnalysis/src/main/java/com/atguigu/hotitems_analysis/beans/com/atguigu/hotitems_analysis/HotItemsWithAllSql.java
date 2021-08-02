package com.atguigu.hotitems_analysis.beans.com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author LiuYang
 * @Date 2021/8/2 2:22 下午
 */
public class HotItemsWithAllSql {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 2. 读取数据，创建DataStream
        DataStreamSource<String> inputStream = env.readTextFile("/Users/liuyang/IdeaProjects/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv");
        // 3. 转换为POJO，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4. 创建表执行环境，用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // 纯SQl
        tableEnv.createTemporaryView("data_table",dataStream,"itemId,behavior, timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                " (select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num" +
                " from (" +
                " select itemId ,count(itemId) as cnt,HOP_END(ts, interval '5' minute ,interval '1' hour) as windowEnd" +
                " from data_table" +
                " where behavior = 'pv' " +
                " group by itemId, HOP(ts ,interval '5' minute, interval '1' hour) " +
                " )" +
                " )" +
                " where row_num <= 5");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute("hot items with sql job");
    }
}
