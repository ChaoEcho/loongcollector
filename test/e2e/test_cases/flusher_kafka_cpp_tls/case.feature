@flusher @tls
@e2e @docker-compose
Feature: flusher kafka cpp TLS
  Skeleton for TLS e2e; requires SSL-enabled Kafka listeners.
  Scenario: TestFlusherKafkaCpp_TLS
    Given {docker-compose} environment
    Given subcribe data from {kafka} with config
    """
    brokers:
      - "localhost:9092"
    topic: "tls-topic"
    """
    Given {flusher-kafka-cpp-tls-case} local config as below
    """
    enable: true
    inputs:
      - Type: input_file
        FilePaths:
          - "/root/test/**/tls_input.log"
        MaxDirSearchDepth: 10
        TailingAllMatchedFiles: true
    processors:
      - Type: processor_parse_json_native
        SourceKey: content
        KeepingSourceWhenParseSucceed: true
    flushers:
      - Type: flusher_kafka_cpp
        Brokers: ["kafka:29093"]
        Topic: "tls-topic"
        Version: "2.8.0"
        Authentication:
          TLS:
            Enabled: true
            CAFile: /etc/kafka/ssl/ca.crt
            InsecureSkipVerify: false
    """
    Given loongcollector container mount {./flusher_tls.log} to {/root/test/1/2/3/tls_input.log}
    Given loongcollector container mount {kafka_certs} to {/etc/kafka/ssl}
    Given loongcollector depends on containers {["kafka", "zookeeper", "cert-generator"]}
    When start docker-compose {flusher_kafka_cpp_tls}
    Then there is at least {10} logs
    Then the log fields match kv
    """
    topic: "tls-topic"
    content: ".*"
    """
