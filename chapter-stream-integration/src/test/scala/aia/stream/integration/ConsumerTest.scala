package aia.stream.integration

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.net.Socket
import java.nio.file.Path

import akka.actor.ActorSystem
import akka.testkit.TestKit
import javax.jms.{Connection, DeliveryMode, Session}

import akka.{Done, NotUsed}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source, Tcp}
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
          .collect {
            case (path, DirectoryChange.Creation) => path
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
    "pickup xml TCPConnection" in {
      import Tcp._

      val msg = new Order("me", "Akka in Action", 10)
      val xml = <order>
                  <customerId>{ msg.customerId }</customerId>
                  <productId>{ msg.productId }</productId>
                  <number>{ msg.number }</number>
                </order>

      val xmlStr = xml.toString.replace("\n", "")

      val tcpSource: Source[IncomingConnection, Future[ServerBinding]] =
        Tcp().bind("localhost", 8887)
          .mapMaterializedValue { binding =>
            import system.dispatcher
            binding.foreach { _ =>
              val socket = new Socket("localhost", 8887)
              // send XML
              val outputWriter = new PrintWriter(socket.getOutputStream, true)
              outputWriter.println(xmlStr)
              outputWriter.flush()
              outputWriter.close()
            }
            binding
          }

      val probe =
        tcpSource.map { connection =>

            val handleFlow =
              Flow[ByteString]
                .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
                .map(_.utf8String)
                .via(parseOrderXmlFlow)
                .alsoToMat(TestSink.probe[Order])(Keep.right)
                .map(_ => ByteString("<confirm>OK</confirm>"))

            connection.handleWith(handleFlow)
          }.toMat(Sink.head)(Keep.right).run()


      Await.result(probe, 10.seconds).requestNext() must be(msg)
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
