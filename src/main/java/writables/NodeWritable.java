package writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NodeWritable implements WritableComparable<NodeWritable> {

    public int id;
    public float distance;
    // flag(_new) : new=true, old=false
    public boolean flag;

    public NodeWritable() {}

    public NodeWritable(int id, float dist, boolean flag)
    {
        this.id = id;
        this.distance = dist;
        this.flag = flag;
    }

    public void set(int id, float dist, boolean flag)
    {
        this.id = id;
        this.distance = dist;
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "NodeDistanceWritable{" +
                "id=" + id +
                ", distance=" + distance +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeFloat(distance);
        dataOutput.writeBoolean(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        distance = dataInput.readFloat();
        flag = dataInput.readBoolean();
    }

    @Override
    public int compareTo(NodeWritable other) {
        if(distance != other.distance)
            return Float.compare(distance, other.distance);
        else if(id != other.id)
            return Integer.compare(id, other.id);
        else
            return Boolean.compare(flag, other.flag);
    }
}