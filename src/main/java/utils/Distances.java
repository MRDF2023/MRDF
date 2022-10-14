package utils;

public class Distances {
    public static float l2(float[] a, float[] b){
        float sum = 0;
        for(int i=0; i<a.length; i++){
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return (float) Math.sqrt(sum);
    }

    public static float l1(float[] a, float[] b)
    {
        float ret = 0.0f;
        for(int i = 0 ; i < a.length ; i++)
        {
            ret += Math.abs(a[i] - b[i]);
        }
        return ret;
    }

    public static float cos(float[] a, float[] b)
    {
        float ret = 0.0f;
        float da = 0.0f, db = 0.0f;
        for(int i = 0 ; i < a.length ; i++)
        {
            ret += (a[i] * b[i]);
            da += a[i] * a[i];
            db += b[i] * b[i];
        }
        if(Math.sqrt(da * db) != 0)
        {
            ret = ret / (float)(Math.sqrt(da * db));
        }
        else ret = 0.0f;
        return 1.0f - (ret);
    }
}
