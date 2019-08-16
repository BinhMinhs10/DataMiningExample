//package model.jpa.mysqlconnector;
//
//import java.util.List;
//
//import javax.persistence.EntityManager;
//import javax.persistence.EntityManagerFactory;
//import javax.persistence.EntityTransaction;
//import javax.persistence.Persistence;
//
//
//public class Main {
//    private static final EntityManagerFactory ENTITY_MANAGER_FACTORY = Persistence
//            .createEntityManagerFactory("JavaHelps");
//
//
//    public static void main(String[] args){
//
////        create(1,"Alice", 22);
////        create(2,"Bob", 20);
////        create(3,"Charlie", 25);
////
////        update(2, "Bob", 25);
////
////        delete(1);
//
//        List students = readAll();
//        if(students != null){
//            for(Object stu : students){
//                System.out.println(stu);
//            }
//        }
//        ENTITY_MANAGER_FACTORY.close();
//    }
//
//    /**
//     * Create a new Student.
//     *
//     * @param name
//     * @param age
//     */
//    public static void create(int id, String name, int age){
//        EntityManager manager = ENTITY_MANAGER_FACTORY.createEntityManager();
//        EntityTransaction transaction = null;
//
//        try{
//            transaction = manager.getTransaction();
//            // Begin the transaction
//            transaction.begin();
//
//            Student stu = new Student();
//            stu.setId(id);
//            stu.setName(name);
//            stu.setAge(age);
//
//            manager.persist(stu);
//            transaction.commit();
//        } catch (Exception ex){
//            if(transaction != null){
//                transaction.rollback();
//            }
//            ex.printStackTrace();
//        }finally {
//            // close the EntityManager
//            manager.close();
//        }
//    }
//
//    /**
//     * Read all the Students.
//     *
//     * @return a List of Students
//     */
//    public static List readAll(){
//        List students = null;
//
//        EntityManager manager = ENTITY_MANAGER_FACTORY.createEntityManager();
//        EntityTransaction transaction = null;
//
//        try{
//            transaction = manager.getTransaction();
//            transaction.begin();
//
//            students = manager.createQuery("SELECT s FROM Student s",
//                    Student.class).getResultList();
//
//            transaction.commit();
//        }catch (Exception ex){
//            if(transaction != null){
//                transaction.rollback();
//            }
//            ex.printStackTrace();
//        }finally {
//            manager.close();
//        }
//        return students;
//    }
//
//    /**
//     * Delete the existing Student.
//     *
//     * @param id
//     */
//    public static void delete(int id) {
//        // Create an EntityManager
//        EntityManager manager = ENTITY_MANAGER_FACTORY.createEntityManager();
//        EntityTransaction transaction = null;
//
//        try {
//            // Get a transaction
//            transaction = manager.getTransaction();
//            // Begin the transaction
//            transaction.begin();
//
//            // Get the Student object
//            Student stu = manager.find(Student.class, id);
//
//            // Delete the student
//            manager.remove(stu);
//
//            // Commit the transaction
//            transaction.commit();
//        } catch (Exception ex) {
//            // If there are any exceptions, roll back the changes
//            if (transaction != null) {
//                transaction.rollback();
//            }
//            // Print the Exception
//            ex.printStackTrace();
//        } finally {
//            // Close the EntityManager
//            manager.close();
//        }
//    }
//
//    /**
//     * Update the existing Student.
//     *
//     * @param id
//     * @param name
//     * @param age
//     */
//    public static void update(int id, String name, int age) {
//        // Create an EntityManager
//        EntityManager manager = ENTITY_MANAGER_FACTORY.createEntityManager();
//        EntityTransaction transaction = null;
//
//        try {
//            // Get a transaction
//            transaction = manager.getTransaction();
//            // Begin the transaction
//            transaction.begin();
//
//            // Get the Student object
//            Student stu = manager.find(Student.class, id);
//
//            // Change the values
//            stu.setName(name);
//            stu.setAge(age);
//
//            // Update the student
//            manager.persist(stu);
//
//            // Commit the transaction
//            transaction.commit();
//        } catch (Exception ex) {
//            // If there are any exceptions, roll back the changes
//            if (transaction != null) {
//                transaction.rollback();
//            }
//            // Print the Exception
//            ex.printStackTrace();
//        } finally {
//            // Close the EntityManager
//            manager.close();
//        }
//    }
//}
