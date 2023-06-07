FROM public.ecr.aws/docker/library/maven:3.9-amazoncorretto-11-debian as builder

COPY src /usr/src/app/src
COPY pom.xml /usr/src/app

RUN mvn -f /usr/src/app/pom.xml clean package

FROM public.ecr.aws/bitnami/java:11

COPY --from=builder /usr/src/app/target/transactionAnalyzer-1.0.jar /usr/app/transactionAnalyzer-1.0.jar

ENTRYPOINT ["java", "-jar", "/usr/app/transactionAnalyzer-1.0.jar"]