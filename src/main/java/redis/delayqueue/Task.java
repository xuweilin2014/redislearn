package redis.delayqueue;

public class Task<T> {
    private String id;
    private T data;

    public Task(String id, T data) {
        this.id = id;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public T getData() {
        return data;
    }
}
