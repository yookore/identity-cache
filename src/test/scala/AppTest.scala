import org.junit.Assert._
import org.junit.Test
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitSuite
import org.apache.spark._
import scala.util.parsing.json._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

class AppTest extends JUnitSuite {

  var vcapApp: String = _
  var vcapServices: String = _

  @Before
  def setup() {
    vcapApp = """
      {
        "VCAP_APPLICATION": {
          "application_name": "uas",
          "application_uris": [
          "uas.apps.yookore.net"
          ],
          "application_version": "aca8f07c-ee8c-40af-b391-610c092f656a",
          "limits": {
          "disk": 1024,
          "fds": 16384,
          "mem": 2048
          },
          "name": "uas",
          "space_id": "e56ef682-e0cb-4b39-a3f5-96c461c414f8",
          "space_name": "dev",
          "uris": [
          "uas.apps.yookore.net"
          ],
          "users": null,
          "version": "aca8f07c-ee8c-40af-b391-610c092f656a"
        }
      }"""

    vcapServices = """
      {
        "VCAP_SERVICES": {
          "ih-cassandra": [
          {
            "credentials": {
            "address": "192.168.10.200",
            "keyspace": "cf_ed3f415e_bcbb_4109_988e_7391926fefb3",
            "password": "e46c28976dab4a14",
            "port": 9160,
            "username": "cf_user_8eb7ad990653078d"
            },
            "label": "ih-cassandra",
            "name": "ih-cassandra-cluster",
            "plan": "best-effort",
            "syslog_drain_url": "",
            "tags": [
            "cassandra",
            "nosql",
            "data"
            ]
          }
          ],
          "user-provided": [
          {
            "credentials": {
            "host": "192.168.10.29",
            "password": "Wordpass15",
            "port": "5672",
            "username": "test"
            },
            "label": "user-provided",
            "name": "rabbitmq",
            "syslog_drain_url": "",
            "tags": []
          },
          {
            "credentials": {
            "host": "192.168.10.98",
            "hosts": [
              "192.168.10.98",
              "192.168.10.5",
              "192.168.10.4"
            ],
            "port": "6379"
            },
            "label": "user-provided",
            "name": "redis",
            "syslog_drain_url": "",
            "tags": []
          },
          {
            "credentials": {
            "db": "uaa",
            "host": "10.10.10.221",
            "password": "P@ssw0rd15",
            "port": "5432",
            "username": "postgres"
            },
            "label": "user-provided",
            "name": "postgresql_ups_idp_dev",
            "syslog_drain_url": "",
            "tags": []
          }
        ]
      }
    }"""
  }

  @Test
  def testVcapSpaceName = {
    val vcapMap = JSON.parseFull(vcapApp)
    val m = vcapMap.get.asInstanceOf[Map[String, String]]
    val rootNode = m.get("VCAP_APPLICATION").get.asInstanceOf[Map[String, String]]
    val spaceName = rootNode.get("space_name")
    println("cf space_name: " + spaceName.get)
    assertNotNull(vcapMap)
  }

  @Test
  def testVcapPostgresService = {
    implicit val formats = DefaultFormats
    val vcapSMap = parse(vcapServices)
    println(vcapSMap.extract[Map[String, _]])
  }

  @Test
  def testSerializeCaseClass = {
    implicit val formats = Serialization.formats(NoTypeHints)
          
                  
      val ser = write(Profile("An avid learner", "165774c2-a0e7-4a24-8f79-0f52bf3e2cda", "2015-07-13T07:48:47.924Z"))
      println(ser)
      assertNotNull(read[Profile](ser))
  }
}

case class Profile(biography: String,
        userid: String,
        creationdate: String) extends java.io.Serializable
