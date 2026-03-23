import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaBroker<T> {

    private final List<Segment<T>> segments = new ArrayList<>();
    private final AtomicLong ProOffset = new AtomicLong(0);
    private volatile Segment<T> CurSegment;
    public int segmentSize = 1000;

    public KafkaBroker() {
        CurSegment = new Segment<>(0);
        segments.add(CurSegment);
    }

    public long append(T data) {

        long offset = ProOffset.getAndIncrement();

        int segmentIndex = (int)(offset / segmentSize);
        int index = (int) (offset % segmentSize);

        Segment<T> segment;

        if (segmentIndex >= segments.size()) {
            synchronized (this) {
                System.out.println("New segment created");
                if (segmentIndex >= segments.size()) {
                    long stOffset = segmentIndex * segmentSize;
                    segment = new Segment<>(stOffset);
                    segments.add(segment);
                    CurSegment = segment;
                }
            }
        }
        segment = segments.get(segmentIndex);
        segment.append(index, data);

        return offset;
    }

    public T consume(long offset) {

        int segmentIndex = (int) (offset / segmentSize);
        int index = (int) (offset % segmentSize);

        if (segmentIndex >= segments.size()) {
            return null;
        }

        Segment<T> segment = segments.get(segmentIndex);
        return segment.read(index);
    }

}



