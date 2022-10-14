package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class NNSimpleWritable implements Writable {

    public int numNeighbors;
    public ArrayList<Integer> neighbors;
    public ArrayList<Float> distances;
    public ArrayList<Boolean> flag_new;

    public void setup()
    {
        if(neighbors == null)
        {
            neighbors = new ArrayList<Integer>();
            distances = new ArrayList<Float>();
            flag_new = new ArrayList<Boolean>();
        }
        numNeighbors = 0;
    }

    public void add(int neighbor, float distance, boolean flag)
    {
        if(numNeighbors < neighbors.size()){
            neighbors.set(numNeighbors, neighbor);
            distances.set(numNeighbors, distance);
            flag_new.set(numNeighbors, flag);
        }
        else {
            neighbors.add(neighbor);
            distances.add(distance);
            flag_new.add(flag);
        }
        numNeighbors+=1;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        numNeighbors = in.readInt();

        if(neighbors == null)
        {
            neighbors = new ArrayList<Integer>(numNeighbors);
            distances = new ArrayList<Float>(numNeighbors);
            flag_new = new ArrayList<Boolean>(numNeighbors);
        }
        else
        {
            neighbors.ensureCapacity(numNeighbors);
            distances.ensureCapacity(numNeighbors);
            flag_new.ensureCapacity(numNeighbors);
        }

        for(int i = neighbors.size(); i < numNeighbors ; i++)
        {
            neighbors.add(-1);
            distances.add(0f);
            flag_new.add(false);
        }

        for(int i = 0 ; i < numNeighbors ; i++)
        {
            neighbors.set(i, in.readInt());
            distances.set(i, in.readFloat());
            flag_new.set(i, in.readBoolean());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(numNeighbors);

        for(int i = 0 ; i < numNeighbors ; i++)
        {
            out.writeInt(neighbors.get(i));
            out.writeFloat(distances.get(i));
            out.writeBoolean(flag_new.get(i));
        }
    }

    @Override
    public String toString() {

        return "R(v) (" + numNeighbors + ") ";
    }
}