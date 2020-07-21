package transaction_manager.raft.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.State;

import java.io.*;

public class StateSnapshot {
    private static final Logger LOG = LoggerFactory.getLogger(StateSnapshot.class);

    private String path;

    static <T> T cast(Object obj) {
        final T t = (T) obj;
        return t;
    }

    public StateSnapshot(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    /**
     *
     * Save value to snapshot file.
     */
    public boolean save(State s) {
        try(ObjectOutputStream out = new ObjectOutputStream(
            new BufferedOutputStream(new FileOutputStream(new File(path))))) {
            out.writeObject(s);
            return true;
        } catch(IOException ioe) {
            LOG.error("Fail to save snapshot", ioe);
            return false;
        }
    }

    public State load() throws IOException {
        File f = new File(path);
        try(ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(f)))) { return cast(in.readObject());
        } catch (ClassNotFoundException e) {
            throw new IOException("Fail to load snapshot from " + path);
        }
    }
}
