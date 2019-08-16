package threadpool;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ScheduledThreadPoolExecutorExample {
    public static void main(String[] args){
        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor)
                Executors.newScheduledThreadPool(2);

        Task tast = new Task("Repeat Task");
        System.out.println("Created: "+ tast.getName());


        // after 2s repeatedly execute
        executor.scheduleWithFixedDelay(tast, 2, 2, TimeUnit.SECONDS);


        //Creates and executes a task that becomes enabled after the given delay
//        executor.schedule(tast, 10, TimeUnit.SECONDS);
//        executor.shutdown();
        // executor at fix rate
//        executor.scheduleAtFixedRate(tast, 2, 2, TimeUnit.SECONDS);

    }
}
