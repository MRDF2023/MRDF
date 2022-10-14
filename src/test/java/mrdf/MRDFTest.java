package mrdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MRDFTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("dimVector",50);
        conf.setInt("numVectors", 10000);

        conf.set("input", "data/test_dataset");
        conf.set("output", "data/mrdf");

        conf.setInt("k", 20);
        conf.setInt("rho", 10);
        conf.setInt("M", 2500);
        conf.setFloat("tau", 0.01f);
        
        ToolRunner.run(conf, new MRDF(), new String[]{});
    }
}
