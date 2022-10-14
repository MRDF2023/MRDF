package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class NodeVectorOrSimpleWritable implements Writable {

    public static int dimVector = 0;

    public int id;

    public boolean hasVector;
    public float[] vector;

    public float distance;
    public boolean flag_new;

    public void set(int id, float[] vector)
    {
        hasVector = true;
        this.id = id;
        this.vector = vector;
    }

    public void set(float distance, boolean flag_new)
    {
        hasVector = false;
        this.distance = distance;
        this.flag_new = flag_new;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        id = in.readInt();

        if(id < 0)
        {
            hasVector = false;
            id = ~id;
            distance = in.readFloat();
            flag_new = in.readBoolean();
        }
        else
        {
            hasVector = true;
            if(vector == null) vector = new float[dimVector];
            for(int i = 0 ; i < dimVector ; i++)
            {
                vector[i] = in.readFloat();
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        if(hasVector)
        {
            out.writeInt(id);
            for(int i = 0 ; i < dimVector ; i++)
            {
                out.writeFloat(vector[i]);
            }
        }
        else
        {
            out.writeInt(~id);
            out.writeFloat(distance);
            out.writeBoolean(flag_new);
        }
    }

}