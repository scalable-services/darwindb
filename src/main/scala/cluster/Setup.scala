package cluster

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.RetentionPolicies

import scala.jdk.CollectionConverters._

object Setup extends Logging {

  def main(args: Array[String]): Unit = {

    // Pass auth-plugin class fully-qualified name if Pulsar-security enabled
    val authPluginClassName = "pulsar"
    // Pass auth-param if auth-plugin class requires it
    val authParams = "param1=value1"
    val useTls = false
    val tlsAllowInsecureConnection = true
    val tlsTrustCertsFilePath = null
    val admin = PulsarAdmin.builder()
      //authentication(authPluginClassName, authParams)
      .serviceHttpUrl(Config.PULSAR_CLIENT_URL)
      .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
      .allowTlsInsecureConnection(tlsAllowInsecureConnection).build()

    val topics = admin.topics()
    val namespaces = admin.namespaces()

    try {

      val NAMESPACE = "public/darwindb"
      val existingNamespaces = namespaces.getNamespaces("public").asScala

      if (!existingNamespaces.exists(_.compareTo(NAMESPACE) == 0)) {
        namespaces.createNamespace(NAMESPACE)
        namespaces.setRetention(NAMESPACE, new RetentionPolicies(-1, -1))
      }

      topics.getList(NAMESPACE).asScala.foreach {
        topics.delete(_, true)
      }
      topics.getPartitionedTopicList(NAMESPACE).asScala.foreach {
        topics.deletePartitionedTopic(_, true)
      }

      admin.topics().createPartitionedTopic(Config.Topics.LOG, Config.NUM_LOG_PARTITIONS)
      admin.topics().createNonPartitionedTopic(Config.Topics.TOPOLOGY_LOG)

      Config.topics.foreach { t => admin.topics().createNonPartitionedTopic(t) }

    } catch {
      case t: Throwable => t.printStackTrace()
    } finally {
      admin.close()
    }

  }

}
