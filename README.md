ShockTrade DataCenter
=====================
ShockTrade DataCenter is the *replacement* processing back-end of the ShockTrade.com web site, which is currently implemented
via a home-grown file ingestion system and Storm. ShockTrade DataCenter is being built atop of [Broadway](https://github.com/ldaniels528/broadway)
as a distributed Actor-based processing system.

## The ShockTrade DataCenter Anthology

The proceeding example is a Broadway anthology performs the following flow:

* Extracts historical stock quotes from a tabbed-delimited file.
* Converts the stock quotes to <a href="http://avro.apache.org/" target="avro">Avro</a> records.
* Publishes each Avro record to a Kafka topic (eoddata.tradinghistory.avro)

Below is the Broadway narrative that implements the flow described above:

```scala
    class EodDataImportNarrative(config: ServerConfig, id: String, props: Properties) extends BroadwayNarrative(config, id, props) {
    
      // extract the properties we need
      val kafkaTopic = props.getOrDie("kafka.topic")
      val zkConnect = props.getOrDie("zookeeper.connect")
    
      // create a file reader actor to read lines from the incoming resource
      lazy val fileReader = prepareActor(new FileReadingActor(config), parallelism = 10)
    
      // create a Kafka publishing actor
      lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 10)
    
      // create a EOD data transformation actor
      lazy val eodDataToAvroActor = prepareActor(new EodDataToAvroActor(kafkaTopic, kafkaPublisher), parallelism = 10)
    
      onStart {
        case Some(resource: ReadableResource) =>
          // start the processing by submitting a request to the file reader actor
          fileReader ! CopyText(resource, eodDataToAvroActor, handler = Delimited("[,]"))
        case _ => 
      }
    }
```

**NOTE:** The `FileReadingActor` and `KafkaPublishingActor` actors are builtin components of Broadway.

The class below is an optional custom actor that will perform the Avro-encoding of the text data before passing the record 
to the Kafka publishing actor (a built-in component).

```scala
    class EodDataToAvroActor(topic: String, kafkaActor: ActorRef) extends Actor {
      private val sdf = new SimpleDateFormat("yyyyMMdd")
    
      override def receive = {
        case OpeningFile(resource) =>
          ResourceTracker.start(resource)
    
        case ClosingFile(resource) =>
          ResourceTracker.stop(resource)
    
        case TextLine(resource, lineNo, line, tokens) =>
          // skip the header line
          if (lineNo != 1) {
            kafkaActor ! PublishAvro(topic, toAvro(resource, tokens))
          }
    
        case message =>
          unhandled(message)
      }
    
      /**
       * Converts the given tokens into an Avro record
       * @param tokens the given tokens
       * @return an Avro record
       */
      private def toAvro(resource: ReadableResource, tokens: Seq[String]) = {
        val items = tokens map (_.trim) map (s => if (s.isEmpty) None else Some(s))
        def item(index: Int) = if (index < items.length) items(index) else None
    
        com.shocktrade.avro.EodDataRecord.newBuilder()
          .setSymbol(item(0).orNull)
          .setExchange(resource.getResourceName.flatMap(extractExchange).orNull)
          .setTradeDate(item(1).flatMap(_.asEPOC(sdf)).map(n => n: JLong).orNull)
          .setOpen(item(2).flatMap(_.asDouble).map(n => n: JDouble).orNull)
          .setHigh(item(3).flatMap(_.asDouble).map(n => n: JDouble).orNull)
          .setLow(item(4).flatMap(_.asDouble).map(n => n: JDouble).orNull)
          .setClose(item(5).flatMap(_.asDouble).map(n => n: JDouble).orNull)
          .setVolume(item(6).flatMap(_.asLong).map(n => n: JLong).orNull)
          .build()
      }
    
      /**
       * Extracts the stock exchange from the given file name
       * @param name the given file name (e.g. "NASDAQ_20120206.txt")
       * @return an option of the stock exchange (e.g. "NASDAQ")
       */
      private def extractExchange(name: String) = name.indexOptionOf("_") map (name.substring(0, _))
    
    }
```

And an XML file to describe how files will be mapped to the narrative:

```xml
<?xml version="1.0" ?>
<anthology id="EodData" version="1.0">

    <narrative id="EodDataImportNarrative"
               class="com.shocktrade.datacenter.narratives.EodDataImportNarrative">
        <properties>
            <property key="kafka.topic">eoddata.tradinghistory.avro</property>
            <property key="zookeeper.connect">dev801:2181</property>
        </properties>
    </narrative>

    <location id="EodData" path="/Users/ldaniels/broadway/incoming/tradingHistory">
        <feed name="AMEX_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
        <feed name="NASDAQ_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
        <feed name="NYSE_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
        <feed name="OTCBB_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
    </location>

</anthology>
```

