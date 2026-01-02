FROM eclipse-temurin:17-jre
COPY target/sqlite-spring-boot.jar /opt/assets/sqlite-spring-boot.jar
WORKDIR /opt/assets
ENV JAVA_OPTS_GC="-server -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+ScavengeBeforeFullGC -XX:+DisableExplicitGC"
ENV JAVA_OPTS="-Xms256m -Xmx256m"
ENV HOSTNAME=client
ENV DB_NAME=orders
EXPOSE 50000
EXPOSE 50001
EXPOSE 29000
EXPOSE 10800 11211 47100 47500 49112 8080
CMD java $JAVA_OPTS_GC $JAVA_OPTS -Dhostname=$HOSTNAME -jar sqlite-spring-boot.jar
