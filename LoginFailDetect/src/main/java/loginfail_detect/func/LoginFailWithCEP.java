package loginfail_detect.func;

/**
 * @author WuChao
 * @create 2021/6/24 20:38
 */

import loginfail_detect.beans.LoginEvent;
import loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * 使用CEP库实现 短时间内连续登录失败检测
 */
public class LoginFailWithCEP {
    public static void main(String[] args)throws Exception {
        // 设置流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从文件中读取数据
        URL resource = LoginFailWithCEP.class.getResource("/LoginLog.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 分配时间戳和WaterMark
        DataStream<LoginEvent> loginEventDataStream = inputStream
                .map(data -> {
                    String[] fields = data.split(",");
                    return new LoginEvent(
                            new Long(fields[0]),
                            fields[1],
                            fields[2],
                            new Long(fields[3])
                    );
                })
                .assignTimestampsAndWatermarks((new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        // 按照 CEP 方法处理

        // 定义一个匹配模式
        // firstFail -> secondFail,在2s时间范围内
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                })
                .next("secondFail").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                })
                .within(Time.seconds(2));

        // 新定义一个匹配模式，处理多次登录失败事件
        Pattern<LoginEvent, LoginEvent> loginFailPattern1 = Pattern
                .<LoginEvent>begin("failEvents").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                })
                .times(3).consecutive() // 严格连续三次登录失败
                .within(Time.seconds(5));

        // 将匹配模式应用到数据流上 得到一个 pattern stream
        PatternStream<LoginEvent> patternStream = CEP
                .pattern(loginEventDataStream.keyBy(LoginEvent::getUseId), loginFailPattern1);

        // 检出符合匹配条件的复杂时间，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.
                select(new LoginFailMatchDetectWarining());


        warningStream.print();

        env.execute("login fail detect with cep job");
    }

    /**
     * 自定义 CEP 的 PatternSelectFunction
     */
    public static class LoginFailMatchDetectWarining implements PatternSelectFunction<LoginEvent, LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
            /*
            LoginEvent firstFailEvent = map.get("firstFail").iterator().next();
            LoginEvent lastFailEvent = map.get("secondFail").get(0);
            return new LoginFailWarning(
                    firstFailEvent.getUseId(),
                    firstFailEvent.getTimestamp(),
                    lastFailEvent.getTimestamp(),
                    "login fail 2 times");

             */
            // 改进版
            LoginEvent firstFailEvent = map.get("failEvents").get(0);
            LoginEvent lastFailEvent = map.get("failEvents").get(map.get("failEvents").size() - 1);

            return new LoginFailWarning(
                    firstFailEvent.getUseId(),
                    firstFailEvent.getTimestamp(),
                    lastFailEvent.getTimestamp(),
                    "login fail " + map.get("failEvents").size() + " times");

        }
    }

}
