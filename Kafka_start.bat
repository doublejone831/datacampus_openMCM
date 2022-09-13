@echo off

START /d "C:\H2Oprojects\kafka_2.13-3.2.0" bin\windows\zookeeper-server-start.bat config\zookeeper.properties

START /d "C:\H2Oprojects\kafka_2.13-3.2.0" bin\windows\kafka-server-start.bat config\server.properties


