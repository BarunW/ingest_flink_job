package sink;

public interface Sink<T>{
    public void write(T record) throws Exception;
}
