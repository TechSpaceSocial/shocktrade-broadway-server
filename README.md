ShockTrade DataCenter
=====================
ShockTrade DataCenter is the *replacement* processing back-end of the ShockTrade.com web site, which is currently implemented
via a home-grown file ingestion system and Storm. ShockTrade DataCenter is being built atop of [Broadway](https://github.com/ldaniels528/broadway)
as a distributed Actor-based processing system.

## The ShockTrade DataCenter Anthology

The proceeding example is a Broadway anthology performs the following flow:

* Extracts stock symbols from a tabbed-delimited file.
* Retrieves stock quotes for each symbol.
* Converts the stock quotes to <a href="http://avro.apache.org/" target="avro">Avro</a> records.
* Publishes each Avro record to a Kafka topic (eoddata.tradinghistory.avro)

Below is the Broadway narrative that implements the flow described above:

```scala
class StockQuoteImportNarrative(config: ServerConfig) extends BroadwayNarrative(config, "Stock Quote Import")
with KafkaConstants {
  // create a file reader actor to read lines from the incoming resource
  lazy val fileReader = addActor(new FileReadingActor(config))

  // create a Kafka publishing actor for stock quotes
  lazy val quotePublisher = addActor(new KafkaAvroPublishingActor(quotesTopic, brokers))

  // create a stock quote lookup actor
  lazy val quoteLookup = addActor(new StockQuoteLookupActor(quotePublisher))

  onStart { resource =>
    // start the processing by submitting a request to the file reader actor
    fileReader ! CopyText(resource, quoteLookup, handler = Delimited("[\t]"))
  }
}
```

**NOTE:** The `KafkaAvroPublishingActor` and `FileReadingActor` actors are builtin components of Broadway.

The class below is an optional custom actor that will perform the stock symbol look-ups and then pass an Avro-encoded
record to the Kafka publishing actor (a built-in component).

```scala
  class EodDataImportNarrative(config: ServerConfig, id: String, props: Properties)
    extends BroadwayNarrative(config, id, props) {
    private lazy val logger = LoggerFactory.getLogger(getClass)
  
    // extract the properties we need
    val kafkaTopic = props.getOrDie("kafka.topic")
    val zkConnect = props.getOrDie("zookeeper.connect")
  
    // create a file reader actor to read lines from the incoming resource
    lazy val fileReader = prepareActor(new FileReadingActor(config), parallelism = 10)
  
    // create a Kafka publishing actor
    lazy val kafkaPublisher = prepareActor(new KafkaPublishingActor(zkConnect), parallelism = 10)
  
    // create a EOD data transformation actor
    val eodDataToAvroActor = prepareActor(new EodDataToAvroActor(kafkaTopic, kafkaPublisher))
  
    onStart {
      _ foreach {
        case resource: ReadableResource =>
          // start the processing by submitting a request to the file reader actor
          fileReader ! CopyText(resource, eodDataToAvroActor, handler = Delimited("[,]"))
        case _ =>
          throw new IllegalStateException(s"A ${classOf[ReadableResource].getName} was expected")
      }
    }
  }
```

And an XML file to describe how files will be mapped to the anthology:

```xml
<?xml version="1.0" ?>
<anthology id="EodData" version="1.0">
    <!-- Narratives -->

    <narrative id="EodDataImportNarrative"
               class="com.shocktrade.datacenter.narratives.EodDataImportNarrative">
        <properties>
            <property key="kafka.topic">eoddata.tradinghistory.avro</property>
            <property key="zookeeper.connect">dev801:2181</property>
        </properties>
    </narrative>

    <!-- Location Triggers -->

    <location id="EodData" path="/Users/ldaniels/broadway/incoming/tradingHistory">
        <feed name="AMEX_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
        <feed name="NASDAQ_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
        <feed name="NYSE_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
        <feed name="OTCBB_(.*)[.]txt" match="regex" narrative-ref="EodDataImportNarrative"/>
    </location>

</anthology>
```

