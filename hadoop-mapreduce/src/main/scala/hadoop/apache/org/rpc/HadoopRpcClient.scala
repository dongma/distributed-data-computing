package hadoop.apache.org.rpc

import java.net.InetSocketAddress

import com.typesafe.scalalogging.Logger
import hadoop.apache.bean.ClientProtocol
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.ipc.RPC

/**
 * @author Sam Ma
 * @date 2021/07/25
 * 构造并启动Rpc Client，向Server端发起请求
 */
object HadoopRpcClient {

  private[this] val logger = Logger(HadoopRpcClient.getClass)

  private val HOST: String = "localhost"

  private val PORT: Int = 2181

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val proxy: ClientProtocol = RPC.getProxy(classOf[ClientProtocol], ClientProtocol.versionID,
      new InetSocketAddress(HOST, PORT), conf)
    var stuName = proxy.getActualName("G20210735010369")
    logger.info("student number: {}, actual name: {}", "G20210735010369", stuName)
    stuName = proxy.getActualName("20210123456789")
    logger.info("student number: {}, actual name: {}", "G20210735010369", stuName)
  }

}
