package com.dkl.blog.avro.kafka

/**
 * Created by dongkelun on 2021/8/18 14:24
 */
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Long,
                        behavior: String,
                        timestamp: Long)
  extends Serializable {

}

object UserBehavior {

  def apply(usrArray: Array[String]): UserBehavior = new UserBehavior(
    usrArray(0).toLong, usrArray(1).toLong, usrArray(2).toLong, usrArray(3), usrArray(4).toLong
  )

}