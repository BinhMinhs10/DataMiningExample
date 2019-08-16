package blockingqueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GFG {
    public class User{
        public String name;
        public String age;
        User(String name, String age){
            this.name = name;
            this.age = age;
        }
    }
    public static void main(String[] args)throws InterruptedException{
        GFG gfg = new GFG();
        gfg.pollMethodUser();

    }

    public void pollMethodInteger() throws InterruptedException{
        int capacity = 5;
        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(capacity);

        queue.offer(423);
        queue.offer(233);
        queue.offer(356);

        System.out.println("Queue Contains"+ queue);

        System.out.println("Removing From head: "+ queue.poll(1, TimeUnit.NANOSECONDS));
        System.out.println("Queue Contains"+ queue);
    }

    public void pollMethodUser(){
        int capacity = 5;
        ArrayBlockingQueue<User> queue = new ArrayBlockingQueue<User>(capacity);

        User user1 = new User("Aman", "24");
        User user3 = new User("Sanjeet", "24");
        queue.offer(user1);
        queue.offer(user3);

        User user = queue.poll();
        System.out.println("removing user having name= "+ user.name);


        User user2 = new User("John", "24");
        queue.add(user2);
        System.out.println("add user having name= "+ user2.name);

        try{
            User head = queue.take();
            System.out.println("removing user having name= "+ head.name);
        }catch (InterruptedException e){
            e.printStackTrace();
        }



    }
}
