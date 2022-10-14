package mrdf;

import writables.DivPathWritable;
import writables.VectorIDWritable;

import java.util.Random;

public class Reservoir {
    public static int div;

    public int cnt;
    public DivPathWritable dpw;
    public VectorIDWritable[] vectors;
    Random rnd;

    public Reservoir(DivPathWritable dpw)
    {
        cnt = 0;
        this.dpw = new DivPathWritable();
        this.dpw.depth = dpw.depth;
        System.arraycopy(dpw.divisions, 0, this.dpw.divisions, 0, dpw.depth);
        vectors = new VectorIDWritable[div];
        for(int i = 0 ; i < div ; i++)
        {
            vectors[i] = new VectorIDWritable();
        }
        rnd = new Random();
    }

    public void update(float[] vec)
    {
        if(cnt < div)
        {
            vectors[cnt].update(vec);
        }
        else
        {
            int tmp = rnd.nextInt(cnt+1);
            if(tmp < div)
            {
                vectors[tmp].update(vec);
            }
        }
        cnt++;
    }
}
