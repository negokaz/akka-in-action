package aia.stream.integration

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.net.Socket
import java.nio.file.Path

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import javax.jms.{Connection, DeliveryMode, Session}
import javax.xml.stream.events.EndDocument

import akka.NotUsed
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.alpakka.xml.{Characters, EndElement, ParseEvent, StartElement}
import akka.stream.alpakka.xml.scaladsl.XmlParsing
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerRegistry
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.xml.XML

class ConsumerTest extends TestKit(ActorSystem("ConsumerTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers {

  implicit val materializer = ActorMaterializer()

  val dir = new File("messages")

  override def beforeAll(): Unit = {
    if (!dir.exists()) {
      dir.mkdir()
    }
    //remove active mq data if it exists
    val mqData = new File("activemq-data")
    if(mqData.exists())
      FileUtils.deleteDirectory(mqData)
  }

  override def afterAll(): Unit = {
    system.terminate()
    FileUtils.deleteDirectory(dir)
  }

  val parseOrderXmlFlow = Flow[String].map { xmlString =>
    val xml = XML.loadString(xmlString)
    val order = xml \\ "order"
    val customer = (order \\ "customerId").text
    val productId = (order \\ "productId").text
    val number = (order \\ "number").text.toInt
    new Order(customer, productId, number)
  }

  "Consumer" must {
    "pickup xml files" in {

      val newFileSource: Source[Path, NotUsed] =
        DirectoryChangesSource(dir.toPath, pollInterval = 500.millis, maxBufferSize = 1000)
          .flatMapConcat {
            case (path, DirectoryChange.Creation) =>
              Source.single(path)
            case _ =>
              Source.empty[Path]
          }

      val msg = new Order("me", "Akka in Action", 10)
      val xml = <order>
                  <customerId>{ msg.customerId }</customerId>
                  <productId>{ msg.productId }</productId>
                  <number>{ msg.number }</number>
                </order>

      val msgFile = new File(dir, "msg1.xml")

      val changes: Future[Order] =
        newFileSource
          .map(path => scala.io.Source.fromFile(path.toFile).mkString)
          .via(parseOrderXmlFlow)
          .runWith(Sink.head[Order])

      FileUtils.write(msgFile, xml.toString())

      Await.result(changes, 10.seconds) must be(msg)
    }
    "pickup xml TCPConnection" ignore {
    }
    "confirm xml TCPConnection" ignore {
    }
    "pickup xml ActiveMQ" ignore {
    }
    "pickup 2 xml files" ignore {
    }
    "pickup 2 xml TCPConnection" ignore {
    }

  }

  "The Producer" must {
    "send msg using TCPConnection" ignore {
    }
    "send Xml using TCPConnection" ignore {
    }
    "receive confirmation when send Xml" ignore {
    }
  }
  def sendMQMessage(msg: String): Unit = {
    // Create a ConnectionFactory
    val connectionFactory =
      new ActiveMQConnectionFactory("tcp://localhost:8899");

    // Create a Connection
    val connection: Connection = connectionFactory.createConnection()
    connection.start()

    // Create a Session
    val session = connection.createSession(false,
      Session.AUTO_ACKNOWLEDGE)

    // Create the destination (Topic or Queue)
    val destination = session.createQueue("xmlTest");

    // Create a MessageProducer from the Session to the Topic or Queue
    val producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    // Create a messages
    val message = session.createTextMessage(msg);

    // Tell the producer to send the message
    producer.send(message);

    // Clean up
    session.close();
    connection.close();
  }
}
