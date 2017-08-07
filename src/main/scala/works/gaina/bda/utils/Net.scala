package works.gaina.bda.utils

import java.net.{InetAddress, NetworkInterface}

/**
  * Created by gaina on 12/13/16.
  */
object Net {

  def getLocalHost: InetAddress =
    InetAddress.getLocalHost

  def getLocalIpAddress: String =
    getLocalHost.getHostAddress

  def getNetworkInterfaces: java.util.Enumeration[NetworkInterface] =
    NetworkInterface.getNetworkInterfaces()

  def getInetAddresses: Unit = {
    val ni = getNetworkInterfaces
    while (ni.hasMoreElements()) {
      val cni = ni.nextElement.getInetAddresses
      println(cni)
    }
  }

}
