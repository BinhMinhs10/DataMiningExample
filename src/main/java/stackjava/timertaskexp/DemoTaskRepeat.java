package stackjava.timertaskexp;

import java.util.Timer;

public class DemoTaskRepeat {
    public static void main(String[] args){
        MyTask myTask = new MyTask();
        Timer timer = new Timer();
        timer.schedule(myTask, 0, 2000);
    }
}
