package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.bean.PageViewCount;
import com.atguigu.networkflow_analysis.bean.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;

/**
 * @Author LiuYang
 * @Date 2021/8/5 2:29 下午
 */

public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 3 转换POJO，分配时间戳和warterMark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 开窗统计UV
        DataStream<PageViewCount> uvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1)) // 因为并行度是1 可以这么用 不建议用timeWindowAll
                .apply(new UvCountResult());

        uvStream.print();
        env.execute();


    }

    private static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义一个Set结构，保存窗口中的所有userId，自动去重
            HashSet<Long> uidSet = new HashSet();
            for(UserBehavior bt: values) {
                uidSet.add(bt.getUserId());
            }
            out.collect(new PageViewCount("uv",window.getEnd(),(long) uidSet.size()));
        }
    }
}
