package wanggang.pub.nifi.processors

import java.io.{IOException, OutputStream}
import java.util

import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.processor.util.StandardValidators

/**
  * Created by wanggang on 2018/4/8.
  */

@SideEffectFree
@Tags(Array("test", "udp"))
@CapabilityDescription("This is a user defined processor.Test for write,modify,append for flowfile content and update attribute.")
class UDProcessor extends AbstractProcessor {

  private var properties: util.List[PropertyDescriptor] = _
  private var relationships: util.Set[Relationship] = _

  //  val MATCH_ATTR: String = "match"

  val WRITEDATA: PropertyDescriptor = new PropertyDescriptor
  .Builder()
    .name("WriteData")
    .description("Data for overwrite into content")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val REL_SUCCESS: Relationship = new Relationship
  .Builder()
    .name("success")
    .description("Success relationship")
    .build
  val REL_FAILURE: Relationship = new Relationship
  .Builder()
    .name("failure")
    .description("Failure relationship")
    .build

  override def init(context: ProcessorInitializationContext): Unit = {
    val props: util.ArrayList[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    props.add(WRITEDATA)
    this.properties = util.Collections.unmodifiableList(props)

    val rels: util.Set[Relationship] = new util.HashSet[Relationship]
    rels.add(REL_SUCCESS)
    rels.add(REL_FAILURE)
    this.relationships = util.Collections.unmodifiableSet(rels)
  }

  @throws[ProcessException]
  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    getLogger.info("This is a user defined processor")
    val flowFile: FlowFile = session.get()
    val wdata = context.getProperty("WriteData").getValue

    session.write(flowFile, new OutputStreamCallback() {
      @throws[IOException]
      override def process(out: OutputStream): Unit = {
        out.write(wdata.getBytes())
      }
    }
    )

    session.putAttribute(flowFile, "test.key", "test.value")
    session.transfer(flowFile, REL_SUCCESS)
  }

  override def getRelationships: util.Set[Relationship] = relationships

  override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = properties
}
