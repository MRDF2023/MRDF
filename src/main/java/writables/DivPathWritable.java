package writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DivPathWritable implements WritableComparable<DivPathWritable> {
    private static int MAX_DEPTH = 30;

    public byte depth;
    public byte[] divisions;

    public DivPathWritable() {
        depth = 0;
        divisions = new byte[MAX_DEPTH];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(depth);

        for(byte i = 0 ; i < depth ; i++)
        {
            dataOutput.writeByte(divisions[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if(divisions == null)
            divisions = new byte[MAX_DEPTH];

        depth = dataInput.readByte();
        for(byte i = 0 ; i < depth ; i++)
        {
            divisions[i] = dataInput.readByte();
        }
    }

    @Override
    public int compareTo(DivPathWritable o) {
        if(this.depth != o.depth) return Short.compare(this.depth, o.depth);
        for(byte i = 0 ; i < this.depth - 1 ; i ++)
        {
            if(this.divisions[i] != o.divisions[i]) return Byte.compare(this.divisions[i], o.divisions[i]);
        }
        return Byte.compare(this.divisions[this.depth-1], o.divisions[this.depth-1]);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(byte i = 0 ; i < depth-1 ; i++)
        {
            sb.append(divisions[i]);
            sb.append(' ');
        }
        return sb.toString() + (divisions[depth-1]);
    }
}
