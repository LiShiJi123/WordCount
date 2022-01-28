package com.LiShiJi.Test

object Helloword {
  def main(args: Array[String]): Unit = {
    for(i <- 3 to 11 if i%2==0)
      {
        print(i+"\t")
      }

  }

}