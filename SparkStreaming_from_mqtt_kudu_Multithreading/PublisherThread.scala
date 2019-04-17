package com.clairvoyant.bigdata.spark

object PublisherThread {

  def main(args: Array[String]): Unit = {

    // while(true){

    var t1 = new Thread(new Runnable {
      override def run(): Unit = {
        var pclass = new MQTTPublisher_class()
        pclass.publish("publisher-1")

      }
    })
    var t2 = new Thread(new Runnable {
      override def run(): Unit = {
        var pclass = new MQTTPublisher_class()
        pclass.publish("publisher-2")

      }
    })

    var t3 = new Thread(new Runnable {
      override def run(): Unit = {
        var pclass = new MQTTPublisher_class()
        pclass.publish("publisher-3")

      }
    })

    var t4 = new Thread(new Runnable {
      override def run(): Unit = {
        var pclass = new MQTTPublisher_class()
        pclass.publish("publisher-4")

      }
    })
    t1.start()
    t2.start()
    t3.start()
    t4.start()

    // }
  }

}
