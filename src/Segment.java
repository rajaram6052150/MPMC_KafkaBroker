import java.util.concurrent.atomic.AtomicReferenceArray;

class Segment<T> {

    private final long startOffset;
    public int segmentId;
    private final AtomicReferenceArray<T> log;

    public Segment(long baseOffset , int segmentId) {
        this.startOffset = baseOffset;
        this.segmentId = segmentId;
        this.log = new AtomicReferenceArray<>(10);
    }

    public void append(int index, T value) {
        log.set(index, value);
    }

    public T read(int index) {
        return log.get(index);
    }

    public long getStartOffset() {
        return startOffset;
    }
}



