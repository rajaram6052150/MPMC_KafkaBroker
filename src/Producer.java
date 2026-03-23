public class Producer implements Runnable{

    KafkaBroker<Integer> broker;
    public int data = 0;

    public Producer(KafkaBroker<Integer> broker){
        this.broker = broker;
    }

    public void run(){
        while (true){
            broker.append(++data);
            try{
                Thread.sleep(200);
            }
            catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}



