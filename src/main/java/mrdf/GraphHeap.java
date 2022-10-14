package mrdf;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

public class GraphHeap{
    public int[][] neighbors;
    public float[][] distances;
    byte[][] flags;
    int n_points, n_neighbors;
    BiFunction<float[], float[], Float> dist;
    Random rand = new Random();

    public GraphHeap(int n_points, int n_neighbors, BiFunction<float[], float[], Float> dist){

        this.n_points = n_points;
        this.n_neighbors = n_neighbors;
        this.dist = dist;

        this.neighbors = new int[n_points][n_neighbors];
        for(int[] n : this.neighbors) Arrays.fill(n, -1);

        this.distances = new float[n_points][n_neighbors];
        for(float[] d : this.distances) Arrays.fill(d, Float.POSITIVE_INFINITY);

        this.flags = new byte[n_points][n_neighbors];
    }

    public void init_rptree(float[][] vectors, int div, int leaf, int epoch){

        List<Integer> nodes = new ArrayList<>();
        for(int i = 0 ; i < n_points ; i++)
            nodes.add(i);

        for(int ep = 0 ; ep < epoch ; ep++)
        {
            divide(vectors, div, leaf, nodes);
        }
    }

    public void divide(float[][] vectors, int div, int leaf, List<Integer> nodes){
        if(nodes.size() <= leaf)
        {
            knn(vectors, nodes);
            return;
        }

        List<Integer>[] divisions = new ArrayList[div];
        for(int i = 0 ; i < div ; i++)
        {
            divisions[i] = new ArrayList<>();
        }

        Collections.shuffle(nodes);
        int[] centroids = new int[div];
        for(int i = 0 ; i < div ; i++)
        {
            centroids[i] = nodes.get(i);
        }

        for(int v : nodes)
        {
            int cnt = 0;
            int minIdx = -1;
            float minDist = Float.MAX_VALUE;
            for(int i = 0 ; i < div ; i++)
            {
                float d = dist.apply(vectors[v], vectors[centroids[i]]);
                if(d == 0.0f) cnt++;
                if(d < minDist)
                {
                    minIdx = i;
                    minDist = d;
                }
            }
            if(cnt == div) minIdx = rand.nextInt(div);

            divisions[minIdx].add(v);
        }

        for(int i = 0 ; i < div ; i++)
        {
            divide(vectors, div, leaf, divisions[i]);
        }
    }

    public void knn(float[][] vectors, List<Integer> nodes){
        for(int i = 0 ; i < nodes.size() ; i++)
        {
            int u = nodes.get(i);
            for(int j = i+1 ; j < nodes.size() ; j++)
            {
                int v = nodes.get(j);
                float d = dist.apply(vectors[u], vectors[v]);
                heap_push_no_dupl(u, v, d, (byte)0);
                heap_push_no_dupl(v, u, d, (byte)0);
            }
        }
    }

    public void init_random(float[][] vectors){
        Random rand = new Random();
        BitSet added = new BitSet();

        for(int u=0; u<n_points; u++){
            added.set(u);
            for(int i=0; i<n_neighbors; i++){
                int v = -1;
                do{
                    v = rand.nextInt(n_points);
                }while(added.get(v));

                added.set(v);
                neighbors[u][i] = v;
            }

            added.clear(u);
            for(int v : neighbors[u])
                added.clear(v);
        }

        IntStream.range(0,n_points).parallel().forEach(u -> {
            for(int i=0; i<n_neighbors; i++){
                int v = neighbors[u][i];
                distances[u][i] = dist.apply(vectors[u], vectors[v]);
            }
        });

        IntStream.range(0,n_points).parallel().forEach(u -> {
            heapify(neighbors[u], distances[u], flags[u]);
        });
    }

    public void heapify(int[] neighbors, float[] distances, byte[] flags){
        for(int i=neighbors.length/2-1; i>=0; i--)
            siftdown(i, neighbors, distances, flags);
    }

    public void siftdown(int pos, int[] neighbors, float[] distances, byte[] flags){

        int me = neighbors[pos];
        float me_dist = distances[pos];
        byte me_flag = flags[pos];
        int child, right;

        while((child = pos*2+1) < distances.length){
            right = child + 1;

            if(right < distances.length && distances[child] < distances[right])
                child++;

            if(me_dist >= distances[child]) break;

            neighbors[pos] = neighbors[child];
            distances[pos] = distances[child];
            flags[pos] = flags[child];
            pos = child;
        }

        neighbors[pos] = me;
        distances[pos] = me_dist;
        flags[pos] = me_flag;

    }

    public int heap_push_no_dupl(int u, int v, float d, byte flag) {
        if(distances[u][0] <= d)
            return 0;

        for(int w : neighbors[u])
            if(w == v) return 0;

        neighbors[u][0] = v;
        distances[u][0] = d;
        flags[u][0] = flag;

        siftdown(0, neighbors[u], distances[u], flags[u]);

        return 1;
    }
}