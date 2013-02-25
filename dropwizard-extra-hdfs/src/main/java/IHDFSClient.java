import java.nio.ByteBuffer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 25/10/2012
 * Time: 12:13
 * To change this template use File | Settings | File Templates.
 *
 */

public interface IHDFSClient {

    public IHDFSClient instance (final String path, final SequenceFile.CompressionType compType, final CompressionCodec codec);
    public void write(Object data);
    public void close();
    public ByteBuffer read();
    public boolean exists(String path);
    public long recordsWritten();
    public Path getPath();


}
