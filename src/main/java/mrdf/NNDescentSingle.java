package mrdf;

import java.util.*;
import java.util.function.BiFunction;

public class NNDescentSingle {

    public static double getRecall(int[][] result, int[][] answer) {
        int n_points = result.length;
        int n_neighbors = result[0].length;

        int right = 0;
        int total = 0;
        Set<Integer> s1 = new HashSet<>();
        Set<Integer> s2 = new HashSet<>();
        for(int u = 0 ; u < n_points ; u++) {
            s1.clear();
            s2.clear();
            for(int i = 0 ; i < n_neighbors ; i++) {
                s1.add(result[u][i]);
                s2.add(answer[u][i]);
            }
            s1.retainAll(s2);
            right += s1.size();
            total += n_neighbors;
        }

        return (double)right / (double)total;
    }

    public static GraphHeap build(float[][] vectors, int n_points, int n_neighbors, int max_iters, int max_candidates, BiFunction<float[], float[], Float> dist, float threshold){

        Random rand = new Random();

        GraphHeap graph = new GraphHeap(n_points, n_neighbors, dist);
        graph.init_rptree(vectors, 5, 2000, 3);

        int[][] old_candidates = new int[n_points][max_candidates];
        int[][] new_candidates = new int[n_points][max_candidates];
        int[] old_sizes = new int[n_points];
        int[] new_sizes = new int[n_points];

        for(byte iter=0; iter < max_iters; iter++){
            for(int i = 0 ; i < n_points ; i++) {
                old_sizes[i] = new_sizes[i] = 0;
            }
            byte new_flag = (byte) (iter+1);

            for(int u=0; u < n_points; u++){
                for(int i=0; i < n_neighbors; i++){
                    int v = graph.neighbors[u][i];
                    if(v == -1) continue;
                    boolean isnew = (graph.flags[u][i] == iter);

                    if(isnew){
                        addInRandom(v, new_candidates[u], new_sizes[u]++, max_candidates, rand);
                        addInRandom(u, new_candidates[v], new_sizes[v]++, max_candidates, rand);
                    }
                    else{
                        addInRandom(v, old_candidates[u], old_sizes[u]++, max_candidates, rand);
                        addInRandom(u, old_candidates[v], old_sizes[v]++, max_candidates, rand);
                    }
                }
            }

            BitSet exist = new BitSet();
            for(int u=0; u < n_points; u++){
                for(int v = 0 ; v < Math.min(new_sizes[u], max_candidates) ; v++) {
                    exist.set(new_candidates[u][v]);
                }

                for(int i=0; i<n_neighbors; i++){
                    int v = graph.neighbors[u][i];
                    if(v == -1) continue;
                    if(!exist.get(v)){
                        graph.flags[u][i] += 1;
                    }
                }

                for(int v = 0 ; v < Math.min(new_sizes[u], max_candidates) ; v++) {
                    exist.clear(new_candidates[u][v]);
                }

            }

            for(int u=0; u < n_points; u++){
                for(int i = 0 ; i < Math.min(new_sizes[u], max_candidates) ; i++){
                    int v = new_candidates[u][i];

                    for(int j = 0 ; j < Math.min(new_sizes[u], max_candidates) ; j++){
                        int w = new_candidates[u][j];
                        if(v == w) continue;
                        float d = dist.apply(vectors[v], vectors[w]);
                        graph.heap_push_no_dupl(v, w, d, new_flag);
                        graph.heap_push_no_dupl(w, v, d, new_flag);
                    }

                    for(int j = 0 ; j < Math.min(old_sizes[u], max_candidates) ; j++){
                        int w = old_candidates[u][j];
                        if(v == w) continue;
                        float d = dist.apply(vectors[v], vectors[w]);
                        graph.heap_push_no_dupl(v, w, d, new_flag);
                        graph.heap_push_no_dupl(w, v, d, new_flag);
                    }

                }
            }

            int num_pushes = 0;
            for(int i = 0 ; i < n_points ; i++) {
                for(int j = 0 ; j < n_neighbors ; j++) {
                    if(graph.flags[i][j] == new_flag) num_pushes++;
                }
            }

            if(num_pushes <= (int)(n_points * n_neighbors * threshold)) break;
        }

        return graph;

    }

    private static boolean addInRandom(int v, int[] candidates, int size, int capacity, Random rand){
        if(size < capacity){
            candidates[size] = v;
            return true;
        }
        else{
            int idx = rand.nextInt(size);
            if(idx < capacity){
                candidates[idx] = v;
                return true;
            }
        }
        return false;
    }
}
