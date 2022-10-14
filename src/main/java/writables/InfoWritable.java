package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoWritable implements Writable {
    public static int div;
    public static int dimVector;

    public int blockSize;
    public float[][] centroids;

    public InfoWritable() {}

    public void initialize() {
        centroids = new float[div][dimVector];
    }

    public void set(int i, float[] ct) {
        System.arraycopy(ct, 0, centroids[i], 0, dimVector);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i < div ; i++)
        {
            for(int j = 0 ; j < dimVector ; j++)
            {
                sb.append(centroids[i][j]);
                sb.append(' ');
            }
            sb.append('\n');
        }
        return sb.toString() + blockSize;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for(int i = 0 ; i < div ; i++)
        {
            for(int j = 0 ; j < dimVector ; j++)
            {
                out.writeFloat(centroids[i][j]);
            }
        }
        out.writeInt(blockSize);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        if(centroids == null) initialize();

        for(int i = 0 ; i < div ; i++)
        {
            for(int j = 0 ; j < dimVector ; j++)
            {
                centroids[i][j] = in.readFloat();
            }
        }
        blockSize = in.readInt();
    }
}
