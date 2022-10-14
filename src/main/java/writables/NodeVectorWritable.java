package writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class NodeVectorWritable implements WritableComparable<NodeVectorWritable> {

    public static int dimVector = 0;

    public int id;
    public boolean flag_new;
    public boolean flag_reverse;
    public boolean flag_sample;
    public float[] vector;

    public NodeVectorWritable(){}

    public NodeVectorWritable(int id, float[] vector, boolean flag_new, boolean flag_reverse, boolean flag_sample){
        set(id, vector, flag_new, flag_reverse, flag_sample);
    }


    public void set(int id, float[] vector, boolean flag_new, boolean flag_reverse, boolean flag_sample){
        this.id = id;
        this.flag_new = flag_new;
        this.flag_reverse = flag_reverse;
        this.flag_sample = flag_sample;
        this.vector = vector;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeBoolean(flag_new);
        out.writeBoolean(flag_reverse);
        out.writeBoolean(flag_sample);
        for(int i = 0; i < dimVector; i++)
            out.writeFloat(vector[i]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        flag_new = in.readBoolean();
        flag_reverse = in.readBoolean();
        flag_sample = in.readBoolean();

        if(vector == null)
            vector = new float[dimVector];

        for(int i = 0; i < dimVector; i++)
            vector[i] = in.readFloat();
    }

    @Override
    public String toString() {
        return "NodeVectorWritable{" +
                "id=" + id +
                ", flag_new=" + flag_new +
                ", flag_reverse=" + flag_reverse +
                ", flag_sample=" + flag_sample +
                ", vector=" + Arrays.toString(vector) +
                '}';
    }

    @Override
    public int compareTo(NodeVectorWritable arg0) {
        return Integer.compare(this.id, arg0.id);
    }
}