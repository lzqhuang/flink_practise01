package cn.flink.practise2;

// 自定义数据类型：URL访问量
class UrlViewCountResult {
    public String url;
    public long windowEnd;
    public long count;

    public UrlViewCountResult(){

    }

    public UrlViewCountResult(String url, long windowEnd, long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public static UrlViewCountResult of(String url, long windowEnd, long count) {
        UrlViewCountResult urlViewCountResult = new UrlViewCountResult();
        urlViewCountResult.url = url;
        urlViewCountResult.windowEnd = windowEnd;
        urlViewCountResult.count = count;
        return urlViewCountResult;
    }

    public String getUrl() {
        return url;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public long getCount() {
        return count;
    }
}
