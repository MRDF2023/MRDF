package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class KnnWritable implements Writable {
    public static int numNeighbors = 0;

    public int[] neighbors;
    public float[] distances;

    public void set(int idx, int id, float dist)
    {
        neighbors[idx] = id;
        distances[idx] = dist;
    }

    public void setup()
    {
        if(neighbors == null)
        {
            neighbors = new int[numNeighbors];
            distances = new float[numNeighbors];
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(neighbors);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        setup();

        for(int i = 0 ; i < numNeighbors ; i++)
        {
            neighbors[i] = in.readInt();
            distances[i] = in.readFloat();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        for(int i = 0 ; i < numNeighbors ; i++)
        {
            out.writeInt(neighbors[i]);
            out.writeFloat(distances[i]);
        }
    }
}

