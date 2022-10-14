package nndmr;

public class NodeVectorDistance implements Comparable<NodeVectorDistance>{
    public int id;
    public float[] vector;
    public float dist;
    public boolean flag_new;

    public NodeVectorDistance(int id, float[] vector, float dist, boolean flag_new){
        this.id = id;
        this.vector = vector;
        this.dist = dist;
        this.flag_new = flag_new;
    }

    @Override
    public int compareTo(NodeVectorDistance other) {
        if(dist != other.dist)
            return Float.compare(dist, other.dist);
        else if(id != other.id)
            return Integer.compare(id, other.id);
        else{
            if(!flag_new) return -1;
            else return 1;
        }
    }

    @Override
    public String toString() {
        return "NodeVectorDistance{" +
                "id=" + id +
                ", dist=" + dist +
                ", flag_new=" + flag_new +
                '}';
    }
}
