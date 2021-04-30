build & unpack source
```
./gradlew :airbyte-thread-source:build
tar xf airbyte-thread-source/build/distributions/airbyte-thread-source-0.22.0-alpha.tar --strip-components=1
```

build & unpack destination
```
 ./gradlew :airbyte-thread-destination:build
tar xf airbyte-thread-destination/build/distributions/airbyte-thread-destination-0.22.0-alpha.tar --strip-components=1
```

run the test
```
 ./bin/airbyte-thread-source | tee /tmp/thread_test.txt | ./bin/airbyte-thread-destination
```

sanity check the pipe output
```
 tail -f  /tmp/thread_test.txt
```