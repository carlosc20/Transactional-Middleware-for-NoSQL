package utils.timer;

import utils.timer.Checkpoint;
import utils.timer.Stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Timer {
    final private ArrayList<Checkpoint> checkpoints;
    final private TimeUnit timeUnit;

    public Timer(){
        this.checkpoints = new ArrayList<>();
        this.timeUnit = TimeUnit.MILLISECONDS;
    }

    public Timer(TimeUnit timeUnit){
        this.checkpoints = new ArrayList<>();
        this.timeUnit = timeUnit;
    }

    public void start(){
	addCheckpoint("Start");
    }

    public synchronized void addCheckpoint(){
        checkpoints.add(new Checkpoint(System.nanoTime()));
    }

    public synchronized void addCheckpoint(String info){
        checkpoints.add(new Checkpoint(System.nanoTime(), info));
    }

    public synchronized void addCheckpoint(String info, String category){
        checkpoints.add(new Checkpoint(System.nanoTime(), info, category));
    }

    private void print(String info){
        if(!info.equals("")) {
            System.out.println("|      \"" + info + "\"");
        }
    }

    private void print(int i, long l1, long l2, String info){
        long total = timeUnit.convert(l1, TimeUnit.NANOSECONDS);
        long last = timeUnit.convert(l2, TimeUnit.NANOSECONDS);
        System.out.println("|");
        System.out.println(String.format("%1$-6s", i) + " total: " + String.format("%1$-10s", total) + ", since last checkpoint: " + String.format("%1$-10s", last));
        print(info);
    }

    public void print(){
        if(checkpoints.size() == 0) {
            System.out.println("No checkpoint added");
            return;
        }
        System.out.println("Timeunit used: " + timeUnit.toString());

        if(checkpoints.size() == 1){
            Checkpoint c = checkpoints.get(0);
            print(1, System.nanoTime() - c.time, 0, c.info);
        }
        else {
            Checkpoint beginning = checkpoints.get(0);
            print(beginning.info);
            for(int i = 1; i<checkpoints.size(); i++){
                Checkpoint c = checkpoints.get(i);
                print(i, c.time - beginning.time, c.time - checkpoints.get(i-1).time, c.info);
            }
        }
    }

    public void printStats() {
        if(checkpoints.size() < 2) {
            System.out.println("Need more than 1 checkpoint for stats");
            return;
        }
        System.out.println("Timeunit used: " + timeUnit.toString());


        Checkpoint beginning = checkpoints.get(0);

        Stats stats = new Stats();
        Map<String,Stats> catStats = new HashMap<>();

        for(int i = 1; i<checkpoints.size(); i++){
            Checkpoint c = checkpoints.get(i);

            //long total = timeUnit.convert(c.time - beginning.time, TimeUnit.NANOSECONDS);
            long sinceLast = timeUnit.convert(c.time - checkpoints.get(i-1).time, TimeUnit.NANOSECONDS);
            String category = c.category;

            stats.addValue(sinceLast);

            if (category != null){
                Stats s = catStats.get(category);
                if(s == null){
                    s = new Stats();
                    catStats.put(category,s);
                }
                s.addValue(sinceLast);
            }
        }
        
        System.out.println("All");
        System.out.println(stats);

        for (Map.Entry<String, Stats> entry : catStats.entrySet()) {
            System.out.println(entry.getKey() + "\n" + entry.getValue());
        }
    }
}
