package model.redis;

import java.io.IOException;
import java.util.*;

import redis.clients.jedis.Jedis;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import model.redis.Student;

public class RedisBase extends TimerTask {

    private Jedis jedis;
    private static final EntityManagerFactory ENTITY_MANAGER_FACTORY = Persistence
            .createEntityManagerFactory("JavaHelps");

    @Override
    public void run() {
        Jedis jedis = new Jedis("localhost");


        /**
         * List all object in redis
         * */
        try {
            Set<byte[]> names = jedis.keys("*Student*".getBytes());
            Iterator<byte[]> it = names.iterator();
            while (it.hasNext()) {
                byte[] s = it.next();

                Student tmp = CommonFunction.deserializeObj(jedis.get(s));
                create(tmp.getId(), tmp.getName(), tmp.getAge());
                System.out.println(tmp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//        Object x = CommonFunction.deserializeObj(jedis.get(serializedKey));
//        System.out.println(x);


        /**
         * List all Onject in Mysql
         * */
        List students = readAll();
        if (students != null) {
            for (Object stu : students) {
                System.out.println(stu);
            }
        }
        ENTITY_MANAGER_FACTORY.close();
    }

    public static void main(String[] args) throws Exception {
        Jedis jedis = new Jedis("localhost");

        String key = genRedisKey("15", "Student");

        Student stu1 = new Student(1, "Minh", 25);
        Student stu2 = new Student(2, "Mai", 28);
        Student stu3 = new Student(3, "Tai", 52);
        Student stu4 = new Student(4, "Son", 23);


        byte[] serializedKey = CommonFunction.serializeObj(key);
        byte[] serializedValue = CommonFunction.serializeObj(stu4);
//        jedis.set(serializedKey, serializedValue);

        // TODO Del key in redis
        try {
            Set<byte[]> names = jedis.keys("*Student*".getBytes());
            Iterator<byte[]> it = names.iterator();
            while (it.hasNext()) {
                byte[] s = it.next();

                Student tmp = CommonFunction.deserializeObj(jedis.get(s));
//                jedis.del(s);

                System.out.println(tmp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String genRedisKey(String msisdn, String clientCode) {
        return clientCode + "_" + msisdn;
    }

    /**
     * Create a new Student.
     *
     * @param name
     * @param age
     */
    public static void create(int id, String name, int age) {
        EntityManager manager = ENTITY_MANAGER_FACTORY.createEntityManager();
        EntityTransaction transaction = null;

        try {
            transaction = manager.getTransaction();
            // Begin the transaction
            transaction.begin();

            Student stu = new Student();
            stu.setId(id);
            stu.setName(name);
            stu.setAge(age);

            manager.persist(stu);
            transaction.commit();
        } catch (Exception ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            ex.printStackTrace();
        } finally {
            // close the EntityManager
            manager.close();
        }
    }

    /**
     * Read all the Students.
     *
     * @return a List of Students
     */
    public static List readAll() {
        List students = null;

        EntityManager manager = ENTITY_MANAGER_FACTORY.createEntityManager();
        EntityTransaction transaction = null;

        try {
            transaction = manager.getTransaction();
            transaction.begin();

            students = manager.createQuery("SELECT s FROM Student s",
                    Student.class).getResultList();

            transaction.commit();
        } catch (Exception ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            ex.printStackTrace();
        } finally {
            manager.close();
        }
        return students;
    }

}
