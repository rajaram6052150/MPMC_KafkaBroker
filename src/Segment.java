import java.util.concurrent.atomic.AtomicReferenceArray;

class Segment<T> {

    private final long startOffset;
    private final AtomicReferenceArray<T> log;

    public Segment(long baseOffset) {
        this.startOffset = baseOffset;
        this.log = new AtomicReferenceArray<>(1000);
    }

    public void append(int index, T value) {
        log.set(index, value);
    }

    public T read(int index) {
        return log.get(index);
    }
}




