package stackjava.timertaskexp;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

public class DemoTaskOnce {
    public static void main(String[] args){
        MyTask myTask = new MyTask();
        Timer timer = new Timer();
        System.out.println("Currnet time: "+ new Date());

        Calendar calender = Calendar.getInstance();
        calender.add(Calendar.SECOND,5);
        Date dateSchedule = calender.getTime();
        timer.schedule(myTask, dateSchedule);



    }
}
