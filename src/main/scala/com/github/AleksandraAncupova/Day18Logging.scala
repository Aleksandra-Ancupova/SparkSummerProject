package com.github.AleksandraAncupova

import org.apache.log4j.Logger

class Day18Logging

object Day18Logging extends App {
  println(classOf[Day18Logging].getName)
  val log = Logger.getLogger(classOf[Day18Logging].getName)
  log.debug("Hello this is a debug message")
  log.info("Hello this is an info message")
  log.warn("This is a warning")
  log.error("This is an error!")

}
