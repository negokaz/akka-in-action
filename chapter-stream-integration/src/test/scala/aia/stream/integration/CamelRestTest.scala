package aia.stream.integration

import java.io._
import java.net.URL

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.XML

class CamelRestTest extends TestKit(ActorSystem("CamelRestTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers {
  implicit val timeout: Timeout = 10 seconds
  implicit val executor = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
  }

  "RestConsumer" must {

    "response when create" ignore {

    }
    "response when request status" ignore {
    }
  }
}
