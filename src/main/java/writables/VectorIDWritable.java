package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class VectorIDWritable implements Writable {

    public static int dimVector = 0;

    public int id;
    public float[] vector;

    public VectorIDWritable()
    {
        id = -1;
        vector = new float[dimVector];
    }
    public VectorIDWritable(int cnt)
    {
        id = cnt;
    }

    public void set(int id)
    {
        this.id = id;
    }
    public void set(float[] vector)
    {
        this.id = -1;
        this.vector = vector;
    }

    public void update(float[] src) {
        this.id = -1;
        System.arraycopy(src, 0, vector, 0, dimVector);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        if(id == -1)
        {
            for(int i = 0 ; i < dimVector ; i++)
            {
                dataOutput.writeFloat(vector[i]);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if(vector == null)
            vector = new float[dimVector];

        id = dataInput.readInt();
        if(id == -1)
        {
            for(int i = 0 ; i < dimVector ; i++)
            {
                vector[i] = dataInput.readFloat();
            }
        }
    }

    @Override
    public String toString() {
        return id + " : " + Arrays.toString(vector);
    }
}
