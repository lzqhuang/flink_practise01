package cn.flink.practise2;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
* ## 页面浏览数统计
    即统计在一段时间内用户访问某个url的次数，输出某段时间内访问量最多的前N个URL。
    如每隔5秒，输出最近10分钟内访问量最多的前N个URL。
*/

public class UrlViewCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<LoginEvent> loginEvent = env.fromElements(
                new LoginEvent("192.168.1.100", "000001", 1511658001L, "GET", "/project/get"),
                new LoginEvent("192.168.1.100", "000002", 1511658001L, "GET", "/project/get"),
                new LoginEvent("192.168.1.100", "000003", 1511658001L, "GET", "/project/del"),
                new LoginEvent("192.168.1.100", "000004", 1511658001L, "GET", "/project/del"),
                new LoginEvent("192.168.1.100", "000005", 1511658001L, "GET", "/project/del"),
                new LoginEvent("192.168.1.100", "000005", 1511658001L, "GET", "/project/post")
        );

        DataStream<LoginEvent> processedDataStream = loginEvent.map((MapFunction<LoginEvent, LoginEvent>) loginEventOb -> {
            loginEventOb.eventTime = loginEventOb.eventTime * 1000;
            return loginEventOb;
        });

        DataStream<LoginEvent> timeStream = processedDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(LoginEvent loginEvent) {
                return loginEvent.eventTime;
            }
        });

        DataStream<UrlViewCountResult> urlViewCountStream = timeStream.keyBy(new KeySelector<LoginEvent, String>() {
                    @Override
                    public String getKey(LoginEvent value) throws Exception {
                        return value.url;
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResultFunction());

        urlViewCountStream.keyBy(
                        new KeySelector<UrlViewCountResult, Long>() {
                            @Override
                            public Long getKey(UrlViewCountResult value) throws Exception {
                                return value.windowEnd;
                            }
                        }
                )
                .process(new TopNUrls(1))
                .print();

        env.execute("Url view count job");
    }
}