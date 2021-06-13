package ca.mcit.bigdata.project4

import java.io.FileNotFoundException
import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path}
import org.apache.hive.jdbc.HiveDriver

trait Config {
  //Hive database connection
  Class.forName(classOf[HiveDriver].getName)
  val connectionString = "jdbc:hive2://172.16.129.58:10000/;user=vish;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()

  //Hadoop
  val conf = new Configuration()
  val hadoopConfDir = "C:\\Users\\Vish\\Desktop\\hadoop"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  val fs = FileSystem.get(conf)

  val stagingPath = new Path("/user/bdss2001/vish1/project4")
  if (fs.exists(stagingPath)) fs.delete(stagingPath, true)
  fs.mkdirs(stagingPath)

  try {
    fs.copyFromLocalFile(new Path("Data\\trips.txt"), new Path("/user/bdss2001/vish1/project4/trips/trips.txt"))
    fs.copyFromLocalFile(new Path("Data\\calendar_dates.txt"), new Path("/user/bdss2001/vish1/project4/calendar_dates/calendar_dates.txt"))
    fs.copyFromLocalFile(new Path("Data\\routes.txt"), new Path("/user/bdss2001/vish1/project4/routes/routes.txt"))
  }
  catch{
    case e : FileNotFoundException =>print("File not found=="+e)
  }
  fs.close()
}
