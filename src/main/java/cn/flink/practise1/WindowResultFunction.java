package cn.flink.practise1;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

    @Override
//    public void apply(Tuple key,  // 窗口的主键，即 itemId
//                      TimeWindow window,  // 窗口
//                      Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
//                      Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
//    ) throws Exception {
    public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {

        Long count = input.iterator().next();
        out.collect(ItemViewCount.of(key, window.getEnd(), count));
    }
}