import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Random;

public class BankATMSimulation {

    static final int ATM_COUNT = 3;
    static final int WORK_SECONDS = 20;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Банк відкрився. Банкоматів: " + ATM_COUNT);

        Bank bank = new Bank(ATM_COUNT);
        Thread schedule = new Thread(new BankSchedule(bank, WORK_SECONDS), "Розклад");
        System.out.println("Стан потоку 'Розклад': " + schedule.getState());
        schedule.start();

        Random rnd = new Random();
        Thread[] clients = new Thread[10];
        for (int i = 1; i <= 10; i++) {
            clients[i - 1] = new Thread(new Client(i, bank, rnd.nextInt(25)), "Клієнт-" + i);
        }
        for (Thread t : clients) t.start();

        Thread.sleep(50);
        System.out.println("Стан потоку 'Клієнт-1' після старту: " + clients[0].getState());

        for (Thread t : clients) t.join();
        schedule.join();

        System.out.println("Стан потоку 'Клієнт-1' після завершення: " + clients[0].getState());
        System.out.println("Робочий день завершено.");
    }
}

class Bank {
    private final Semaphore atms;
    private final AtomicBoolean open = new AtomicBoolean(true);

    Bank(int count) { atms = new Semaphore(count, true); }

    Semaphore atms() { return atms; }
    boolean isOpen() { return open.get(); }
    void close() { open.set(false); System.out.println(">>> Банк зачинився."); }
}

class BankSchedule implements Runnable {
    private final Bank bank;
    private final int seconds;

    BankSchedule(Bank bank, int seconds) { this.bank = bank; this.seconds = seconds; }

    public void run() {
        try {
            TimeUnit.SECONDS.sleep(seconds);
            bank.close();
        } catch (InterruptedException e) {
            System.out.println("Розклад банку перервано: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}

class Client implements Runnable {
    private final int id;
    private final Bank bank;
    private final int delay;
    private final Random rnd = new Random();

    Client(int id, Bank bank, int delay) { this.id = id; this.bank = bank; this.delay = delay; }

    public void run() {
        try {
            TimeUnit.SECONDS.sleep(delay);
            System.out.println("Клієнт " + id + " прийшов до банку.");

            if (!bank.isOpen()) {
                System.out.println("Клієнт " + id + ": банк зачинений.");
                return;
            }

            if (!bank.atms().tryAcquire(5, TimeUnit.SECONDS)) {
                System.out.println("Клієнт " + id + ": не дочекався банкомата.");
                return;
            }

            try {
                if (!bank.isOpen()) {
                    System.out.println("Клієнт " + id + ": банк зачинився, поки чекав.");
                    return;
                }
                System.out.println("Клієнт " + id + " користується банкоматом...");
                TimeUnit.SECONDS.sleep(1 + rnd.nextInt(3));
                System.out.println("Клієнт " + id + " зняв готівку і пішов.");
            } finally {
                bank.atms().release();
            }

        } catch (InterruptedException e) {
            System.out.println("Клієнт " + id + ": помилка потоку - " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
