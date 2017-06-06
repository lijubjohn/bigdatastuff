#!/usr/bin/env bash
java -jar avro-tools-1.8.2.jar fromjson ../src/main/resources/students.json --schema-file ../src/main/resources/student.avsc > ../src/main/resources/student.avro