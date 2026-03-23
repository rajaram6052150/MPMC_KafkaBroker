public class Main{
    public static void main(String[] args){

        KafkaBroker<Integer> broker = new KafkaBroker<>();

        for (int i = 0 ; i < 4 ; i++){
            new Thread(new Producer(broker)).start();
        }

        for (int i = 0 ; i < 3 ; i++){
            new Thread(new Consumer(broker)).start();
        }

    }
}




