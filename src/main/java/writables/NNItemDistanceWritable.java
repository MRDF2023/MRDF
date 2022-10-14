package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class NNItemDistanceWritable implements Writable {

    public static int dimVector = 0;

    public int id;
    public int numNeighbors;

    public float[] vector;

    public ArrayList<Integer> neighbors;
    public ArrayList<Float> distances;
    public ArrayList<Boolean> flag_new;
    public ArrayList<Boolean> flag_reverse;
    public ArrayList<Boolean> flag_sample;

    public ArrayList<float[]> vectors;


    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(id);
        out.writeInt(numNeighbors);

        for(float i : vector)
            out.writeFloat(i);

        for(int i = 0; i < numNeighbors; i++){
            out.writeBoolean(flag_new.get(i));
            out.writeBoolean(flag_reverse.get(i));
            out.writeBoolean(flag_sample.get(i));
        }

        for(int i = 0; i < numNeighbors; i++){
            out.writeInt(neighbors.get(i));
            out.writeFloat(distances.get(i));
            float[] vec = vectors.get(i);
            for(float v : vec)
                out.writeFloat(v);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        numNeighbors = in.readInt();

        if(vector == null)
            vector = new float[dimVector];

        if(neighbors == null){
            neighbors = new ArrayList<>(numNeighbors);
            distances = new ArrayList<>(numNeighbors);
            flag_new = new ArrayList<>(numNeighbors);
            flag_reverse = new ArrayList<>(numNeighbors);
            flag_sample = new ArrayList<>(numNeighbors);
            vectors = new ArrayList<>(numNeighbors);
        }
        else {
            neighbors.ensureCapacity(numNeighbors);
            distances.ensureCapacity(numNeighbors);
            vectors.ensureCapacity(numNeighbors);
            flag_new.ensureCapacity(numNeighbors);
            flag_reverse.ensureCapacity(numNeighbors);
            flag_sample.ensureCapacity(numNeighbors);
        }

        for(int i = neighbors.size(); i < numNeighbors; i++){
            neighbors.add(0);
            distances.add(Float.POSITIVE_INFINITY);
            flag_new.add(false);
            flag_reverse.add(false);
            flag_sample.add(false);
            vectors.add(new float[dimVector]);
        }

        for(int i = 0; i < dimVector; i++)
            vector[i] = in.readFloat();

        for(int i = 0; i < numNeighbors; i++){
            flag_new.set(i, in.readBoolean());
            flag_reverse.set(i, in.readBoolean());
            flag_sample.set(i, in.readBoolean());
        }

        for(int i = 0; i < numNeighbors; i++){
            neighbors.set(i, in.readInt());
            distances.set(i, in.readFloat());

            float[] vec = vectors.get(i);
            for(int j = 0; j < dimVector; j++)
                vec[j] = in.readFloat();
        }
    }

    public void add(int id, float[] vector, float distance, boolean flag_new, boolean flag_reverse, boolean flag_sample) {
        if(numNeighbors < neighbors.size()){
            neighbors.set(numNeighbors, id);
            distances.set(numNeighbors, distance);
            this.flag_new.set(numNeighbors, flag_new);
            this.flag_reverse.set(numNeighbors, flag_reverse);
            this.flag_sample.set(numNeighbors, flag_sample);
            float[] vec = vectors.get(numNeighbors);
            System.arraycopy(vector, 0,vec, 0, dimVector);
        }
        else {
            neighbors.add(id);
            distances.add(distance);
            this.flag_new.add(flag_new);
            this.flag_reverse.add(flag_reverse);
            this.flag_sample.add(flag_sample);
            vectors.add(vector.clone());
        }
        numNeighbors+=1;
    }

    public void setVector(int id, float[] vector) {
        this.id = id;
        System.arraycopy(vector, 0, this.vector, 0, dimVector);
    }

    public void initialize() {
        id = -1;
        numNeighbors = 0;

        if(vector == null)
            vector = new float[dimVector];

        if(neighbors == null){
            neighbors = new ArrayList<>();
            distances = new ArrayList<>();
            vectors = new ArrayList<>();
            flag_new = new ArrayList<>();
            flag_reverse = new ArrayList<>();
            flag_sample = new ArrayList<>();
        }
    }

    @Override
    public String toString() {
        return "NNItemWritable{" +
                "id=" + id +
                ", numNeighbors=" + numNeighbors +
                ", neighbors=" + neighbors.subList(0, numNeighbors) +
                ", flag_new=" + flag_new.subList(0, numNeighbors) +
                ", flag_reverse=" + flag_reverse.subList(0, numNeighbors) +
                ", flag_sample=" + flag_sample.subList(0, numNeighbors) +
                ", vector=" + Arrays.toString(vector) +
                ", vectors=" + vectors.subList(0, numNeighbors) +
                '}';
    }
}
