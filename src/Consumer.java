public class Consumer implements Runnable{

    public KafkaBroker<Integer> broker;
    public int consumerOffset = 0;
    public Consumer(KafkaBroker<Integer> broker) {
        this.broker = broker;
    }

    public void run(){
        while (true){
            System.out.println("Consumed " + broker.consume(consumerOffset));
            consumerOffset++;
            try{
                Thread.sleep(200);
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}



