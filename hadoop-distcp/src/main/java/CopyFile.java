import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liju on 2/14/17.
 */
public class CopyFile {

    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://localhost:9000/");

            List<Path> srcPathList = new ArrayList<Path>();
            srcPathList.add(new Path("hdfs://vp21q40ic-hpao081316.me.com:50001/home/hadoop/local_ireporter/raw/CalDAV/CalDAV_pool-1-thread-5_vp21q42ic-ibam061311.me.com_8015_7008_06-02-17.dat.2017-02-06-15"));

            Path targetPath = new Path("http://localhost:50070/user/hadoop/output/");

            DistCpOptions distCpOptions = new DistCpOptions(srcPathList,targetPath);
            DistCp distCp = new DistCp(configuration,distCpOptions);
            Job job = distCp.execute();
            System.out.println(job.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
