package utils;

public class Checkpoint {
    String info;
    long time;
    String category;

    public Checkpoint(long currentTime){
        this.time = currentTime;
        this.info = "";
    }

    public Checkpoint(long currentTime, String info){
        this.time = currentTime;
        this.info = info;
    }

    public Checkpoint(long currentTime, String info, String category){
        this.time = currentTime;
        this.info = info;
        this.category = category;
    }
}
