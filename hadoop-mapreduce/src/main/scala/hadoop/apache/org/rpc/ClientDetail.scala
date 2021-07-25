package hadoop.apache.org.rpc

import com.typesafe.scalalogging.Logger
import hadoop.apache.bean.ClientProtocol
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.ipc.RPC.Server
import org.apache.hadoop.ipc.{ProtocolSignature, RPC}

/**
 * @author Sam Ma
 * @date 2021/07/25
 * 定义hadoop rpc接口，实现server端的业务逻辑
 */
class ClientDetail extends ClientProtocol {

  /** 输入G20210735010369，返回马冬；输入20210123456789，返回心心 */
  override def getActualName(studentId: String): String = {
    var stuName: String = "unknown student"
    if ("G20210735010369".equals(studentId)) {
      stuName = "马冬"
    } else if ("20210123456789".equals(studentId)) {
      stuName = "心心"
    }
    stuName
  }

  override def getProtocolVersion(s: String, l: Long): Long = {
    ClientProtocol.versionID
  }

  override def getProtocolSignature(s: String, l: Long, i: Int): ProtocolSignature = {
    new ProtocolSignature(ClientProtocol.versionID, null)
  }

}

/** 构建并启动Hadoop Rpc Server */
object RpcServer {

  private[this] val logger = Logger(RpcServer.getClass)

  private val HOST: String = "localhost"

  private val PORT: Int = 2181

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val server: Server = new RPC.Builder(conf).setProtocol(classOf[ClientProtocol])
      .setInstance(new ClientDetail()).setBindAddress(HOST)
      .setPort(PORT)
      .setNumHandlers(2).build()
    server.start()
    logger.info("start Hadoop RpcServer which bind with localhost and 2181 port")
  }

}

