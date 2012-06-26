package com.datasift.dropwizard.kafka.consumer

import com.yammer.dropwizard.util.Duration
import util.matching.Regex.Groups
import org.codehaus.jackson.map.annotate.JsonDeserialize
import org.codehaus.jackson.map.{DeserializationContext, JsonDeserializer}
import org.codehaus.jackson.{JsonToken, JsonParser}

/** factory for ErrorActions */
object ErrorPolicy {

  /** parse the specified String in to an ErrorPolicy or die trying */
  def apply(policy: String): ErrorPolicy = {
    val res = for (
      action <- Shutdown(policy) orElse Restart(policy);
      Groups(delay) <- "%s\\s+after\\s+(\\w+)".format(action.name).r.findFirstMatchIn(policy)
    ) yield ErrorPolicy(action, Duration.parse(delay))

    res getOrElse {
      throw new IllegalArgumentException("invalid error policy '%s'".format(policy))
    }
  }
}

@JsonDeserialize(using = classOf[ErrorPolicyDeserializer])
case class ErrorPolicy(action: ErrorAction, delay: Duration = Duration.seconds(0))

/** Nasty work-around for Jerkson's sub-par handling of case classes */
class ErrorPolicyDeserializer extends JsonDeserializer[ErrorPolicy] {
  def deserialize(jp: JsonParser, ctxt: DeserializationContext): ErrorPolicy = {
    if (jp.getCurrentToken != JsonToken.VALUE_STRING) {
      throw ctxt.mappingException(classOf[ErrorPolicy])
    }

    ErrorPolicy(jp.getText)
  }
}


sealed abstract class ErrorAction(val name: String) {
  def apply(action: String): Option[ErrorAction] = {
    if (action.startsWith(name)) Option(this) else None
  }
}
case object Shutdown extends ErrorAction("shutdown")
case object Restart extends ErrorAction("restart")
