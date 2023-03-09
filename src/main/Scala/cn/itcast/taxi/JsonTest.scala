package cn.itcast.taxi

import org.json4s.jackson.Serialization

object JsonTest {

  def main(args: Array[String]): Unit = {

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization.{read, write}

    val product =
      """
        |{"name":"Toy","price":35.35}
  """.stripMargin

//    println(parse(product) \ "name")

    // 隐式转换的形式提供格式工具，例如，如何解析时间字符串
    implicit val formats = Serialization.formats(NoTypeHints)

    // 具体解析为某一个对象
    val productObj1 = parse((product)).extract[Product]

    // 可以通过一个方法，直接将JSOn字符串转为对象，但是这种方式无法进行搜索
    val productObj2 = read[Product](product)

    // 将对象转换为JSON字符串
    val productObj3 = Product("电视", 10.5)
//    val jsonStr1 = compact(render(productObj3))  // render去除非法值 compact转化为字符串
    val jsonStr = write(productObj3)

    println(jsonStr)
  }

}

case class Product(name: String, price: Double)

