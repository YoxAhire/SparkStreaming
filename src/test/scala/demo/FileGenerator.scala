package demo
import org.apache.commons.io.FileUtils

import java.io.File

class FileGenerator(dataDir : String) extends Runnable{
  override def run(): Unit = {
    var fileNumber = 0
    while(true){
      FileUtils.writeStringToFile(new File(s"${dataDir}/${fileNumber}.txt"),fileNumber.toString)
      fileNumber += 1
      Thread.sleep(5000l)
    }
  }
}
