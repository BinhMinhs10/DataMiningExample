package model.redis;

import stackjava.timertaskexp.MyTask;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

public class AppTaskDaily {
    public static void main(String[] args) {
        RedisBase myTask = new RedisBase();

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 17);
        calendar.set(Calendar.MINUTE, 48);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);


        Date dateSchedule = calendar.getTime();
        long period = 24 * 60 * 60 * 1000;

        Timer timer = new Timer();
        timer.schedule(myTask, dateSchedule, period);
    }
}
