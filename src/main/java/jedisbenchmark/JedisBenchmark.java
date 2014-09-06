package jedisbenchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JedisBenchmark {

    private static class SetBenchmarkRunnable implements Runnable {

        int iterations = 100;
        Redis redis;
        String name;

        private SetBenchmarkRunnable(String name, int iterations, Redis redis) {
            this.iterations = iterations;
            this.redis = redis;
            this.name = name;
        }

        @Override
        public void run() {
            for(int i = 0; i < iterations; i++) {
                redis.set(UUID.randomUUID().toString(), "" + System.currentTimeMillis());
                if(i % 10000 == 0) {
                    System.out.println(name + ": " + i + " sets done");
                }
            }

        }
    }

    private static class GetBenchmarkRunnable implements Runnable {

        int iterations = 100;
        Redis redis;
        String name;

        private GetBenchmarkRunnable(String name, int iterations, Redis redis) {
            this.iterations = iterations;
            this.redis = redis;
            this.name = name;
        }

        @Override
        public void run() {
            for(int i = 0; i < iterations; i++) {
                redis.get("foo");
                if(i % 10000 == 0) {
                    System.out.println(name + ": " + i + " gets done");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 4) {
            System.out.println("Usage: jedis-benchmark <set|get> <iterations> <jedis-pool-size> <redis-host>");
        }
        String type = args[0];
        int iterations = Integer.parseInt(args[1]);
        int threadCount = Runtime.getRuntime().availableProcessors();
        int poolSize = Integer.parseInt(args[2]);
        String host = args[3];
        System.out.println(threadCount + " threads " + iterations + " iterations in total");
        System.out.println("Jedis host: " + host);
        System.out.println("Jedis pool size: " + poolSize);
        System.out.println("Operation type: " + type);
        Redis redis = new Redis(host, poolSize);
        long took = runBenchmark(type, iterations, threadCount, redis);
        System.out.println(iterations + " operations in " + took + "ms");
        int setsPerSecond = (int) (iterations / (took / 1000.0));
        System.out.println(setsPerSecond + " sets/s");
    }

    private static long runBenchmark(String type, int iterations, int threadCount, Redis redis) throws InterruptedException {
        List<Thread> threads = new ArrayList<>(threadCount);
        for(int i = 0; i < threadCount; i++) {
            Runnable runnable;
            if(type.equals("get")) {
                runnable = new GetBenchmarkRunnable("Thread " + i, iterations / threadCount, redis);
            } else {
                runnable = new SetBenchmarkRunnable("Thread " + i, iterations / threadCount, redis);
            }
            Thread thread = new Thread(runnable);
            threads.add(thread);
        }
        long start = System.currentTimeMillis();
        for(Thread thread : threads) {
            thread.start();
        }
        for(Thread thread : threads) {
            if(thread.isAlive()) {
                thread.join();
            }
        }
        long stop = System.currentTimeMillis();
        return stop - start;
    }

}
