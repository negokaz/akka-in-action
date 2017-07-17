package aia.stream.integration

import akka.actor.Props
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.xml.NodeSeq

abstract class OrderServiceTest extends WordSpec
    with Matchers 
    with OrderService
    with ScalatestRouteTest {

  implicit val executionContext = system.dispatcher
  implicit val requestTimeout = akka.util.Timeout(1 second)

  "The order service" should {
    "return NotFound if the order cannot be found" ignore {
      Get("/orders/1") ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "return the tracking order for an order that was posted" ignore {
      val xmlOrder = 
      <order><customerId>customer1</customerId>
        <productId>Akka in action</productId>
        <number>10</number>
      </order>
      
      Post("/orders", xmlOrder) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val xml = responseAs[NodeSeq]
        val id = (xml \\ "id").text.toInt
        val orderStatus = (xml \\ "status").text
        id shouldEqual 1
        orderStatus shouldEqual "received"
      }
      Get("/orders/1") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        val xml = responseAs[NodeSeq]
        val id = (xml \\ "id").text.toInt
        val orderStatus = (xml \\ "status").text
        id shouldEqual 1
        orderStatus shouldEqual "processing"
      }
    }
  }
} 