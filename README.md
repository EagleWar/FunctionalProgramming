# FunctionalProgramming

This our project in Functional Programming.

We use Kafka to produce and consumme real time data record by drones.

We use Spark to analyse the data stored in parquet format.

Finally, SparkStreaming is our consummer and check if there is a problem with drones battery. If the battery is under 20%, we have a consummer js which alert sending an email.

To run this project, you have to lauch sbt in Produce folder and Consummer folder.

To have analysis, run sbt as well in Analytics folder.

And to have alert monitoring, run the nodejs file in folder Consummer.

All the project works with:

sbt 0.13.9
kafka 2.11-2.2.0
