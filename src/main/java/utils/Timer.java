package utils;

import java.util.ArrayList;
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

    public void addCheckpoint(){
        checkpoints.add(new Checkpoint(System.nanoTime()));
    }

    public synchronized void addCheckpoint(String info){
        checkpoints.add(new Checkpoint(System.nanoTime(), info));
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
        System.out.println("Timeunit used: " + timeUnit.toString());
        if(checkpoints.size() == 0)
            System.out.println("No checkpoint added");
        else if(checkpoints.size() == 1){
            Checkpoint c = checkpoints.get(0);
            print(1, System.nanoTime() - c.time, 0, c.info);
        }
        else{
            Checkpoint beginning = checkpoints.get(0);
            print(beginning.info);
            for(int i = 1; i<checkpoints.size(); i++){
                Checkpoint c = checkpoints.get(i);
                print(i, c.time - beginning.time, c.time - checkpoints.get(i-1).time, c.info);
            }
        }
        this.checkpoints.clear();
    }
}
