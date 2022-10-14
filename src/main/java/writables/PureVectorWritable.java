package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PureVectorWritable implements Writable {

    public static int dimVector = 0;

    public int id;
    public float[] vector;

    public PureVectorWritable()
    {
        id = -1;
        vector = new float[dimVector];
    }

    public void set(int id, float[] vector){
        this.id = id;
        this.vector = vector;
    }

    public void update(int id, float[] src) {
        this.id = id;
        System.arraycopy(src, 0, vector, 0, dimVector);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i < dimVector ; i++) {
            sb.append(vector[i]);
            sb.append(' ');
        }
        return id + " " + sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        for (int i = 0; i < dimVector; i++)
            out.writeFloat(vector[i]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (vector == null)
            vector = new float[dimVector];

        id = in.readInt();
        for (int i = 0; i < dimVector; i++)
            vector[i] = in.readFloat();
    }
}
