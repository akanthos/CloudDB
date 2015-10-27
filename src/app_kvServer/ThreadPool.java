package app_kvServer;

import java.util.LinkedList;

public class ThreadPool {

    /**
     * List of threads in the threadpool
     */
    protected Thread threads[] = null;
    protected LinkedList<Runnable> jobs = new LinkedList<Runnable>();


    public ThreadPool(int size)
    {
        threads = new Thread[size];

        for (int i = 0; i < size; i++) {
            threads[i] = new WorkerThread(this);
            threads[i].start();
        }
    }

    public int getNumWorkerThreads() {

        return threads.length;
    }

    public int getNumJobs() {

        return jobs.size();
    }


    public synchronized void addToQueue(Runnable r) throws InterruptedException {

        jobs.push(r);
        this.notify();

        return;
    }

    /**
     * Blocking until a job is available in the queue and then retrieve the job
     * @return The runnable task that has to be executed
     * @throws InterruptedException
     */
    public synchronized Runnable getJob() throws InterruptedException {

        while (jobs.size() == 0) {
            this.wait();
        }

        return jobs.pop();
    }

}

/**
 * The worker threads that make up the thread pool.
 */
class WorkerThread extends Thread {

    private ThreadPool pool = null;

    /**
     * The constructor.
     *
     * @param o the thread pool
     */
    WorkerThread(ThreadPool o)
    {
        pool = o;
    }

    /**
     * Scan for executing tasks.
     */
    public void run()
    {
        while (true) {
            try {
                Runnable new_job = pool.getJob();
                new_job.run();
            } catch (InterruptedException e) {
                //dunno if needed
                return;
            }
        }

    }
}
