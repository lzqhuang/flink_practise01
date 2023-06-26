package cn.flink.practise2;



import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 自定义函数：窗口函数
// KEY var1, W var2, Iterable<IN> var3, Collector<OUT> var4
// 键的类型、窗口的类型，输入类型以及输出类型
// public class WindowResultFunction implements WindowFunction<Long, Tuple2<Long, Long>, Tuple, TimeWindow> {
public class WindowResultFunction implements WindowFunction<Long, UrlViewCountResult, String, TimeWindow> {

    @Override
//    public void apply(Tuple key,  // 窗口的主键，即 itemId
//                      TimeWindow window,  // 窗口
//                      Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
//                      Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
//    ) throws Exception {
    public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlViewCountResult> out) throws Exception {

        Long count = input.iterator().next();
        out.collect(new UrlViewCountResult(url, window.getEnd(), count));
    }
}