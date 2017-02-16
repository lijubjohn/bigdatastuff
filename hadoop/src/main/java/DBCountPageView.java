import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.hsqldb.Server;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;
import java.util.Random;


/**
 * Created by liju on 2/15/17.
 *
 * Simple example to view page count
 */
public class DBCountPageView extends Configured implements Tool{

    private static Logger LOG = Logger.getLogger(DBCountPageView.class);
    private static final String DB_URL = "jdbc:hsqldb:hsql://localhost/URLAccess";
    private static final String DRIVER_CLASS = "org.hsqldb.jdbc.JDBCDriver";

    private Server server;
    private Connection connection;
    private boolean initialized = false;


    private void startHQSLServer(){
        server = new Server();
        server.setDatabasePath(0,"/tmp/hsqldb/URLAccess");
        server.setDatabaseName(0, "URLAccess");
        server.start();
    }

    private void createConnections(String driverClassname,String url) throws ClassNotFoundException, SQLException {
        Class.forName(driverClassname);
        connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
    }

    private void shutdown(){
        try {
            if (connection!=null)
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (server!=null)
            server.shutdown();
    }

    private void initialize(String driverClassName,String url) throws SQLException, ClassNotFoundException {

        if (!this.initialized){
            if(driverClassName.equals(DRIVER_CLASS)){
                startHQSLServer();
                createConnections(driverClassName,url);
                dropTables();
                createTables();
                populateAccessTable();
                this.initialized = true;
            }
        }
    }

    private void dropTables() {
        String dropAccess = "DROP TABLE Access";
        String dropPageview = "DROP TABLE PageView";
        Statement st = null;
        try {
            st = connection.createStatement();
            st.executeUpdate(dropAccess);
            st.executeUpdate(dropPageview);
            connection.commit();
            st.close();
        }catch (SQLException ex) {
            try { if (st != null) { st.close(); } } catch (Exception e) {}
        }
    }

    private void createTables() throws SQLException {

        String createAccess =
                "CREATE TABLE " +
                        "Access(url      VARCHAR(100) NOT NULL," +
                        " referrer VARCHAR(100)," +
                        " time     BIGINT NOT NULL, " +
                        " PRIMARY KEY (url, time))";

        String createPageview =
                "CREATE TABLE " +
                        "PageView(url      VARCHAR(100) NOT NULL," +
                        " pageview     BIGINT NOT NULL, " +
                        " PRIMARY KEY (url))";

        Statement st = connection.createStatement();
        try {
            st.executeUpdate(createAccess);
            st.executeUpdate(createPageview);
            connection.commit();
        } finally {
            st.close();
        }
    }

    private void populateAccessTable() throws SQLException {

        PreparedStatement statement = null ;
        try {
            statement = connection.prepareStatement(
                    "INSERT INTO Access(url, referrer, time)" +
                            " VALUES (?, ?, ?)");

            Random random = new Random();

            int time = random.nextInt(50) + 50;

            final int PROBABILITY_PRECISION = 100; //  1 / 100
            final int NEW_PAGE_PROBABILITY  = 15;  //  15 / 100


            //Pages in the site :
            String[] pages = {"/a", "/b", "/c", "/d", "/e",
                    "/f", "/g", "/h", "/i", "/j"};
            //linkMatrix[i] is the array of pages(indexes) that page_i links to.
            int[][] linkMatrix = {{1,5,7}, {0,7,4,6,}, {0,1,7,8},
                    {0,2,4,6,7,9}, {0,1}, {0,3,5,9}, {0}, {0,1,3}, {0,2,6}, {0,2,6}};

            //a mini model of user browsing a la pagerank
            int currentPage = random.nextInt(pages.length);
            String referrer = null;

            for(int i=0; i<time; i++) {

                statement.setString(1, pages[currentPage]);
                statement.setString(2, referrer);
                statement.setLong(3, i);
                statement.execute();

                int action = random.nextInt(PROBABILITY_PRECISION);

                // go to a new page with probability
                // NEW_PAGE_PROBABILITY / PROBABILITY_PRECISION
                if(action < NEW_PAGE_PROBABILITY) {
                    currentPage = random.nextInt(pages.length); // a random page
                    referrer = null;
                }
                else {
                    referrer = pages[currentPage];
                    action = random.nextInt(linkMatrix[currentPage].length);
                    currentPage = linkMatrix[currentPage][action];
                }
            }

            connection.commit();

        }catch (SQLException ex) {
            connection.rollback();
            throw ex;
        } finally {
            if(statement != null) {
                statement.close();
            }
        }
    }

    /**Verifies the results are correct */
    private boolean verify() throws SQLException {
        //check total num PageView
        String countAccessQuery = "SELECT COUNT(*) FROM Access";
        String sumPageviewQuery = "SELECT SUM(pageview) FROM PageView";
        Statement st = null;
        ResultSet rs = null;
        try {
            st = connection.createStatement();
            rs = st.executeQuery(countAccessQuery);
            rs.next();
            long totalPageview = rs.getLong(1);

            rs = st.executeQuery(sumPageviewQuery);
            rs.next();
            long sumPageview = rs.getLong(1);

            LOG.info("totalPageview=" + totalPageview);
            LOG.info("sumPageview=" + sumPageview);

            return totalPageview == sumPageview && totalPageview != 0;
        }finally {
            if(st != null)
                st.close();
            if(rs != null)
                rs.close();
        }
    }


    static class AccessRecord implements Writable,DBWritable{
        private String url;
        private String referrer;
        private long time;

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, url);
            Text.writeString(out, referrer);
            out.writeLong(time);
        }

        public void readFields(DataInput in) throws IOException {
            url = Text.readString(in);
            referrer = Text.readString(in);
            time = in.readLong();
        }

        public void write(PreparedStatement statement) throws SQLException {
            statement.setString(1,url);
            statement.setString(2,referrer);
            statement.setLong(3,time);
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            url = resultSet.getString(1);
            referrer = resultSet.getString(2);
            time = resultSet.getLong(3);
        }

        @Override
        public String toString() {
            return "AccessRecord{" +
                    "url='" + url + '\'' +
                    ", referrer='" + referrer + '\'' +
                    ", time=" + time +
                    '}';
        }
    }

    static class PageViewRecord implements Writable,DBWritable{
        private String url;
        private Long pageView;

        public PageViewRecord(String url, Long pageView) {
            this.url = url;
            this.pageView = pageView;
        }

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, url);
            out.writeLong(pageView);
        }

        public void readFields(DataInput in) throws IOException {
            url = Text.readString(in);
            pageView = in.readLong();
        }

        public void write(PreparedStatement statement) throws SQLException {
            statement.setString(1,url);
            statement.setLong(2,pageView);
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            url = resultSet.getString(1);
            pageView = resultSet.getLong(2);
        }
    }

    /**
     * Mapper
     */
    static class PageViewMapper extends Mapper<LongWritable,AccessRecord,Text,LongWritable> {

        private final static LongWritable ONE  = new LongWritable(1);
        @Override
        protected void map(LongWritable key, AccessRecord value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value.url),ONE);
        }
    }


    /**
     * Reducer
     */
    static class PageViewReducer extends Reducer<Text,LongWritable,PageViewRecord,NullWritable>{

        private final static NullWritable nullWritable = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            PageViewRecord pageViewRecord = new PageViewRecord(key.toString(),sum);
            context.write(pageViewRecord, nullWritable);
        }
    }


    /**
     * Driver method
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        String driverClassName = DRIVER_CLASS;
        String url = DB_URL;

        if(args.length > 1) {
            driverClassName = args[0];
            url = args[1];
        }

        initialize(driverClassName, url);
        Configuration conf = getConf();
        DBConfiguration.configureDB(conf,driverClassName,url);

        final Job job = Job.getInstance(conf);
        job.setJobName("Count pageViews by url");
        job.setJarByClass(DBCountPageView.class);
        job.setMapperClass(PageViewMapper.class);
        job.setReducerClass(PageViewReducer.class);
        job.setCombinerClass(LongSumReducer.class);

        //Input output format
        DBInputFormat.setInput(job,AccessRecord.class,"Access",null,"url",new String[]{"url", "referrer", "time"});
        DBOutputFormat.setOutput(job,"PageView",new String[]{"url","pageview"});

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(PageViewRecord.class);
        job.setOutputValueClass(NullWritable.class);

        int ret;
        try {
            ret = job.waitForCompletion(true) ? 0 : 1;
            boolean correct = verify();
            if(!correct) {
                throw new RuntimeException("Evaluation was not correct!");
            }
        } finally {
            shutdown();
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new DBCountPageView(), args);
        System.exit(ret);
    }
}
