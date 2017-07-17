package aia.stream.integration

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.xml.XML


case class TrackingOrder(id: Long, status: String, order: Order)

case class OrderId(id: Long)

case class NoSuchOrder(id: Long)
