package hotitems_analysis.hotItems;

import hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.types.Row;

/**
 * @author WuChao
 * @create 2021/6/20 15:19
 */
public class HotItemsWithSql {
    /**
     * 使用 Flink SQL来操作
     *
     */
    public static void main(String[] args) throws Exception {
        // 创建流式执行环境 并配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从 文件 读取数据
        DataStream<String> inputStream = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv");

        // 转化为POJO类型 分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
            // 日志数据已经经过了ETL清洗，按照时间戳升序排序
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L; // 时间戳毫秒数
            }
        });

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream,"itemId,behavior,timestamp.rowtime as ts");

        // 分组开窗
        Table windowAggTable = dataTable
                .filter("behavior = 'pv' ")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as cnt");
        // 没法直接注册 windowAggTable 需要转成 DataStream 再注册成表
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg",aggStream,"itemId,windowEnd,cnt");

        // SQL
        Table resultTable = tableEnv.sqlQuery("select * from" +
                "(select *,ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num" +
                " from agg )" +
                "where row_num <=5 ");

//        tableEnv.toRetractStream(resultTable, Row.class).print();


        // 纯SQL
        tableEnv.createTemporaryView("data_table", dataStream, "itemId,behavior,timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from" +
                "(  select *,ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "   from " +
                "   (" +
                "       select itemId,count(itemId) as cnt,HOP_END(ts,interval '5' minute,interval '1' hour ) as windowEnd " +
                "       from data_table " +
                "       where behavior = 'pv' " +
                "       group by itemId,HOP(ts,interval '5' minute,interval '1' hour ) " +
                "   )" +
                ")" +
                "where row_num <= 5");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();


        env.execute("hot items with sql job");



    }
}
