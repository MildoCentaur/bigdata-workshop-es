# ETL: US stocks analysis



### Create a jar containing your application and its deps
```bash
$ sbt clean assembly
```

### Use spark-submit to run your application

```bash
$ spark-submit --master 'spark://master:7077' \
--class "ar.edu.itba.seminario.BCRADataGenerator" \
 --driver-class-path /app/postgresql-42.1.4.jar \
--total-executor-cores 3 \
target/scala-2.11/DolarArgentina-assembly-0.1.jar
```

```bash
$ spark-submit --master 'spark://master:7077' \
--class "ar.edu.itba.seminario.DolarETLProcessor" \
 --driver-class-path /app/postgresql-42.1.4.jar \
--total-executor-cores 3 \
target/scala-2.11/DolarArgentina-assembly-0.1.jar
```