package jraft.snapshot;

import java.io.*;

import certifier.CertifierImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterSnapshotFile {
    private static final Logger LOG = LoggerFactory.getLogger(CounterSnapshotFile.class);

    private String              path;

    static <T> T cast(Object obj, Class<T> clazz) {
        return clazz.isInstance(obj)? clazz.cast(obj): null;
    }

    static <T> T cast(Object obj) {
        final T t = (T) obj;
        return t;
    }

    public CounterSnapshotFile(String path) {
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
    public boolean save(CertifierImpl s) {
        try(ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(new File(path))))) {
            out.writeObject(s);
            return true;
        } catch(IOException ioe) {
            LOG.error("Fail to save snapshot", ioe);
            return false;
        }
    }

    public CertifierImpl load() throws IOException {
        File f = new File(path);
        try(ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(f)))) {
            return cast(in.readObject());
        } catch (ClassNotFoundException e) {
            throw new IOException("Fail to load snapshot from " + path);
        }
    }
}
