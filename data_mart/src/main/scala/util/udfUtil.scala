package util

import java.net.{URL, URLDecoder}
import scala.util.Try


object udfUtil {
  val getCategoryAge: Integer => String = {
    case a if (a >= 18 && a <= 24) => "18-24"
    case a if (a >= 25 && a <= 34) => "25-34"
    case a if (a >= 35 && a <= 44) => "35-44"
    case a if (a >= 45 && a <= 54) => "45-54"
    case a if (a >= 55) => ">=55"
  }

  val getHostDecodeUrl: String => Option[String] = (url: String) => {
    val urlClean = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25")
      .replaceAll("\\+", "%2B")
    val decoder = URLDecoder.decode(urlClean, "UTF-8")
    Try(new URL(decoder).getHost).toOption
  }
}
