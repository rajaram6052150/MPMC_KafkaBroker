import java.util.ArrayList;
import java.util.List;

public class Main{
    public static void main(String[] args){

        List<Consumer> consumers = new ArrayList<>();
        KafkaBroker<Integer> broker = new KafkaBroker<>();

        for (int i = 0 ; i < 4 ; i++){
            new Thread(new Producer(broker)).start();
        }

        for (int i = 0 ; i < 3 ; i++){
            Consumer c =  new Consumer(broker);
            Thread t = new Thread(c);
            consumers.add(c);
            t.start();
        }

        broker.setConsumers(consumers);
    }
}







