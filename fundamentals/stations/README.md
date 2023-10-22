<br>

### Maven Project Object Model (POM)

The full structure of the ``<build></build>`` is:

```xml
<build>
    <sourceDirectory>
    </sourceDirectory>
    <testSourceDirectory>        
    </testSourceDirectory>
    <plugins>
        <plugin>
        </plugin>
        <plugin>
        </plugin>
    </plugins>
    <finalName>
    </finalName>
</build>
```

<br>

### Stations

Objectives:

* Unload stations data
* Upload its contents into a database

<br>

#### Data Schema

The [schema file](./src/main/resources/schemaOfSource.json) is in development.  Once ready, read and view its contents via

```
  // The schema of the data in question
  val schemaProperties: Try[RDD[String]] = Exception.allCatch.withTry(
    spark.sparkContext.textFile(localSettings.resourcesDirectory + "schemaOfSource.json")
  )
  
  // The StructType form of the schema
  val schema: StructType = if (schemaProperties.isSuccess) {
    DataType.fromJson(schemaProperties.get.collect.mkString("")).asInstanceOf[StructType]
  } else {
    sys.error(schemaProperties.failed.get.getMessage)
  }
  schema.printTreeString()
```

<br>

#### Inferred Schema

View
```
  data.printSchema()
```

Write to directory `schema`
```
  spark.sparkContext.parallelize(data.schema)
    .coalesce(1).saveAsTextFile("schema")
```

<br>
<br>

<br>
<br>

<br>
<br>

<br>
<br>
