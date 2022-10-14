package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class NodeOrNodeVectorWritable implements Writable {

    public static int dimVector = 0;

    public int id;
    public float[] vector;
    public boolean hasVector;

    public void set(int id, float[] vector){
        this.id = id;
        this.vector = vector;
        this.hasVector = true;
    }

    public void set(int id){
        this.id = id;
        this.hasVector = false;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        if(hasVector){
            out.writeInt(id);
            for(int i = 0; i < dimVector; i++)
                out.writeFloat(vector[i]);
        }
        else{
            out.writeInt(~id);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        if(id < 0){
            hasVector = false;
            id = ~id;
        }
        else{
            hasVector = true;

            if(vector == null)
                vector = new float[dimVector];

            for(int i = 0; i < dimVector; i++)
                vector[i] = in.readFloat();
        }
    }

    @Override
    public String toString() {
        if(hasVector){
            return "NodeOrNodeVectorWritable{" +
                    "id=" + id +
                    ", vector=" + Arrays.toString(vector) +
                    '}';
        }
        else{
            return "NodeOrNodeVectorWritable{" + "id=" + id + '}';
        }

    }
}