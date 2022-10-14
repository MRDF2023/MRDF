package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KNNSimpleItemWritable implements Writable {

    public static int numNeighbors = 0;

    public int[] neighbors;
    public float[] distances;
    public boolean[] flag_new;

    public void setup()
    {
        if(neighbors == null)
        {
            neighbors = new int[numNeighbors];
            distances = new float[numNeighbors];
            flag_new = new boolean[numNeighbors];
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        setup();

        for(int i = 0 ; i < numNeighbors ; i++)
        {
            neighbors[i] = in.readInt();
            distances[i] = in.readFloat();
            flag_new[i] = in.readBoolean();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        for(int i = 0 ; i < numNeighbors ; i++)
        {
            out.writeInt(neighbors[i]);
            out.writeFloat(distances[i]);
            out.writeBoolean(flag_new[i]);
        }
    }
}