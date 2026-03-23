import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class KafkaBroker<T> {

    public ConcurrentHashMap<Integer , Segment<T>> segments;
    private final AtomicLong ProOffset = new AtomicLong(0);
    private volatile Segment<T> CurSegment;
    public int segmentSize = 10;
    public static int idCounter = -1;
    public int RetentionID = 0;
    public List<Consumer> consumers;
    ScheduledExecutorService RetentionMonitor = Executors.newScheduledThreadPool(1);
    Logger logger = Logger.getLogger(KafkaBroker.class.getName());

    public KafkaBroker() {
        CurSegment = new Segment<>(0 , ++idCounter);
        segments = new ConcurrentHashMap<>();
        segments.put(idCounter , CurSegment);
        RetentionMonitor.scheduleAtFixedRate(() -> DeleteOldSegments() , 2 , 5 , TimeUnit.SECONDS);
    }

    public long append(T data) {

        long offset = ProOffset.getAndIncrement();

        int segmentIndex = (int)(offset / segmentSize);
        int index = (int) (offset % segmentSize);
        Segment<T> segment;

        if (!segments.containsKey(segmentIndex)) {
            synchronized (this) {
                if (!segments.containsKey(segmentIndex)) {
                    long stOffset = segmentIndex * segmentSize;
                    segment = new Segment<>(stOffset , ++idCounter);
                    segments.put(idCounter , segment);
                    CurSegment = segment;
                    logger.info("New Segment Created " + segmentIndex);
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

    public void setConsumers(List<Consumer> consumers){
        this.consumers = consumers;
    }

    public void DeleteOldSegments() {

        int minConOffset = Integer.MAX_VALUE;

        for (Consumer c : consumers) {
            minConOffset = Math.min(minConOffset, c.getConsumerOffset());
        }

        if (segments.get(RetentionID).getStartOffset() + segmentSize < minConOffset) {
            segments.remove(RetentionID);
            ++RetentionID;
            logger.severe("Segment " + (RetentionID - 1) + " is deleted");
        }
    }
}






