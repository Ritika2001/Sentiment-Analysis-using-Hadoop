import java.io.IOException;
import java.util.Properties;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class script1
{
	public static void main(String args[]) 
	{
		try{
		Properties props = new Properties();
		props.setProperty("fs.default.name", "hdfs://localhost:9000/");
		props.setProperty("mapred.job.tracker", "localhost:8088");
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE, props);
		
		
			pigServer.registerScript("/home/ritika/NLP/scriptmapred.pig");
		} catch (Exception e){e.printStackTrace();}
	}

}



