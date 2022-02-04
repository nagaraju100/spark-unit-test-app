***Spark unit test App***

- Application input and output paths are in **application-dev.conf** file.
- Sample data is kept in resources/input

**How to Run**
1. Create jar file  with Maven.
2. Copy the created jar file in some folder or in the target folder (here I have copied to some folder)
3. Run the jar with default environment , following way.(Have run locally)
``` spark-submit --packages com.typesafe:config:1.2.1 --class com.nagaraju.example.jobs.EmployeeData --master local[*] .\spark-unit-test-app-1.0-SNAPSHOT.jar ```
4. Run the jar with dev environment
``` spark-submit --packages com.typesafe:config:1.2.1 --class com.nagaraju.example.jobs.EmployeeData --master local[*] --conf "spark.driver.extraJavaOptions=-DAPP_ENV=dev" --conf "spark.executor.extraJavaOptions=-DAPP_ENV=dev" .\spark-unit-test-app-1.0-SNAPSHOT.jar```

5. Run the jar with qa environment
``` spark-submit --packages com.typesafe:config:1.2.1 --class com.nagaraju.example.jobs.EmployeeData --master local[*] --conf "spark.driver.extraJavaOptions=-DAPP_ENV=qa" --conf "spark.executor.extraJavaOptions=-DAPP_ENV=qa" .\spark-unit-test-app-1.0-SNAPSHOT.jar```

5. Run the jar with prod environment
``` spark-submit --packages com.typesafe:config:1.2.1 --class com.nagaraju.example.jobs.EmployeeData --master local[*] --conf "spark.driver.extraJavaOptions=-DAPP_ENV=prod" --conf "spark.executor.extraJavaOptions=-DAPP_ENV=prod" .\spark-unit-test-app-1.0-SNAPSHOT.jar```

6. Run the jar file with custom log4j property file 
``` spark-submit  --jars ".\config-1.2.1.jar" --class com.nagaraju.example.jobs.EmployeeData --master local[*] --conf "spark.driver.extraJavaOptions=-DAPP_ENV=dev" --conf "spark.executor.extraJavaOptions=-DAPP_ENV=dev"--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --files log4j.properties .\spark-unit-test-app-1.0-SNAPSHOT.jar```
7. Run spark hive example 
```spark-submit --jars config-1.2.1.jar --master local[*] --class com.nagaraju.example.jobs.SparkHiveExample spark-unit-test-app-1.0-SNAPSHOT.jar```
