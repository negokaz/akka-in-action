package aia.stream.integration

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.net.Socket
import java.nio.file.{Files, Path}

import aia.stream.integration.ProcessOrders._
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.amqp.{AmqpConnectionUri, AmqpSinkSettings, NamedQueueSourceSettings, OutgoingMessage}
import akka.stream.alpakka.amqp.scaladsl.{AmqpSink, AmqpSource}
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource
import akka.stream.scaladsl.{Flow, Framing, Keep, RunnableGraph, Sink, Source, Tcp}
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import com.rabbitmq.client.{AMQP, ConnectionFactory}
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

      val newFileContentSource: Source[String, NotUsed] =
        DirectoryChangesSource(dir.toPath, pollInterval = 500.millis, maxBufferSize = 1000)
          .collect {
            case (path, DirectoryChange.Creation) => path
          }
          .map(_.toFile)
          .filter(file => file.isFile && file.canRead)
          .map(scala.io.Source.fromFile(_).mkString)

      val consumer: RunnableGraph[Future[Order]] =
        newFileContentSource
          .via(parseOrderXmlFlow)
          .toMat(Sink.head[Order])(Keep.right)

      val consumedOrder: Future[Order] = consumer.run()

      val msg = new Order("me", "Akka in Action", 10)
      val xml = <order>
                  <customerId>{ msg.customerId }</customerId>
                  <productId>{ msg.productId }</productId>
                  <number>{ msg.number }</number>
                </order>
      val msgFile = new File(dir, "msg1.xml")

      FileUtils.write(msgFile, xml.toString())

      Await.result(consumedOrder, 10.seconds) must be(msg)
    }
    "confirm xml TCPConnection" in {
      import Tcp._
      implicit val executionContext = system.dispatcher

      val msg = new Order("me", "Akka in Action", 10)
      val xml = <order>
                  <customerId>{ msg.customerId }</customerId>
                  <productId>{ msg.productId }</productId>
                  <number>{ msg.number }</number>
                </order>

      val xmlStr = xml.toString.replace("\n", "")

      val tcpSource: Source[IncomingConnection, Future[ServerBinding]] =
        Tcp().bind("localhost", 8887)

      val (serverBindingFuture, orderProbeFuture) =
        tcpSource.map { connection =>

          val confirm = Source.single("<confirm>OK</confirm>\n")

          val handleFlow =
            Flow[ByteString]
              .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
              .map(_.utf8String)
              .via(parseOrderXmlFlow)
              .alsoToMat(TestSink.probe[Order])(Keep.right)
              .merge(confirm)
              .map(c => ByteString(c.toString))

          connection.handleWith(handleFlow)
        }.toMat(Sink.head)(Keep.both).run()

      val responseFuture = serverBindingFuture.map { _ =>
        // サーバーサイドのソケットがバインドされた後、
        // クライアントサイドのソケットを作成し、リクエストを送信
        val socket = new Socket("localhost", 8887)

        val outputWriter = new PrintWriter(socket.getOutputStream)
        val responseReader = new BufferedReader(new InputStreamReader(socket.getInputStream))

        outputWriter.println(xmlStr)
        outputWriter.flush()
        val response = responseReader.readLine()
        responseReader.close()
        outputWriter.close()

        response
      }

      Await.result(orderProbeFuture, 10.seconds).requestNext() must be(msg)
      Await.result(responseFuture, 20.seconds) must be("<confirm>OK</confirm>")
    }
    "pickup xml RabbitMQ" in {
      val queueName = "xmlTest"
      val amqpSettings =
        NamedQueueSourceSettings(
          AmqpConnectionUri("amqp://localhost:8899"),
          queueName
        )
      val amqpSource: Source[String, NotUsed] =
          AmqpSource(amqpSettings, bufferSize = 10)
            .map(_.bytes.utf8String)

      val msg = new Order("me", "Akka in Action", 10)
      val xml = <order>
                  <customerId>{ msg.customerId }</customerId>
                  <productId>{ msg.productId }</productId>
                  <number>{ msg.number }</number>
                </order>

      sendMQMessage(queueName, xml.toString)

      val consumer: RunnableGraph[Future[Order]] =
        amqpSource
          .via(parseOrderXmlFlow)
          .toMat(Sink.head)(Keep.right)

      val consumedOrder: Future[Order] = consumer.run()
      Await.result(consumedOrder, 10 seconds) must be(msg)
    }
  }

  "The Producer" must {
    "send msg using TCPConnection" in {
      implicit val executionContext = system.dispatcher

      val serverBindingFuture =
        Tcp().bind("localhost", 8887).map { connection =>
          val handleFlow =
            Flow[ByteString]
              .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
              .map(_.utf8String)
              .via(parseOrderXmlFlow)
              .merge(Source.single("<confirm>OK</confirm>"))
              .map(c => ByteString(c.toString))
          connection.handleWith(handleFlow)
        }.to(Sink.ignore).run()

      val msg = new Order("me", "Akka in Action", 10)
      val xml = <order>
                  <customerId>{ msg.customerId }</customerId>
                  <productId>{ msg.productId }</productId>
                  <number>{ msg.number }</number>
                </order>
      val xmlStr = xml.toString().replace("\n", "")

      val probeFuture =
        serverBindingFuture.map { _ =>

          val handleFlow =
            Flow[ByteString]
                .alsoToMat(TestSink.probe)(Keep.right)
                .merge(Source.single(xmlStr))
                .map(c => ByteString(c.toString))

          val connection = Tcp().outgoingConnection("localhost", 8887)
          connection.joinMat(handleFlow)(Keep.right).run()
        }

      Await.result(probeFuture, 10 seconds).requestNext().utf8String must be ("<confirm>OK</confirm>")
    }

    "send msg using AMQP" in {
      implicit val executionContext = system.dispatcher

      val queueName = "xmlTest"

      val amqpSinkSettings =
        AmqpSinkSettings(
          AmqpConnectionUri("amqp://localhost:8899"),
          routingKey = Some(queueName)
        )
      val amqpSink: Sink[ByteString, Future[Done]] =
        AmqpSink.simple(amqpSinkSettings)

      val msg = new Order("me", "Akka in Action", 10)
      val xml = <order>
                  <customerId>{ msg.customerId }</customerId>
                  <productId>{ msg.productId }</productId>
                  <number>{ msg.number }</number>
                </order>

      val producer: RunnableGraph[NotUsed] =
        Source.single(xml)
              .map(msg => ByteString(msg.toString))
              .to(amqpSink)

      val amqpSourceSettings =
        NamedQueueSourceSettings(
          AmqpConnectionUri("amqp://localhost:8899"),
          queueName
        )
      val amqpSource: Source[String, NotUsed] =
        AmqpSource(amqpSourceSettings, bufferSize = 10)
          .map(_.bytes.utf8String)

      val consumer: RunnableGraph[Future[Order]] =
        amqpSource
          .via(parseOrderXmlFlow)
          .toMat(Sink.head)(Keep.right)

      producer.run()

      val consumedOrder: Future[Order] = consumer.run()
      Await.result(consumedOrder, 10 seconds) must be(msg)
    }
  }
  def sendMQMessage(queueName: String, msg: String): Unit = {

    // Create a ConnectionFactory
    val connectionFactory = new ConnectionFactory
    connectionFactory.setUri("amqp://localhost:8899")

    // Create a Connection
    val connection = connectionFactory.newConnection()

    // Create a Channel
    val channel = connection.createChannel()
    channel.queueDeclare(queueName, true, false, false, null)

    // send the message
    channel.basicPublish("", queueName,
      new AMQP.BasicProperties.Builder().build(),
      msg.getBytes()
    )

    // Clean up
    channel.close()
    connection.close()
  }
}
