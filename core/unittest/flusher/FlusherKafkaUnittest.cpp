/*
 * Copyright 2025 iLogtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define APSARA_UNIT_TEST_MAIN

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "collection_pipeline/CollectionPipelineContext.h"
#include "common/memory/SourceBuffer.h"
#include "models/LogEvent.h"
#include "models/PipelineEventGroup.h"
#include "plugin/flusher/kafka/FlusherKafka.h"
#include "unittest/Unittest.h"

using namespace std;

namespace logtail {

class FlusherKafkaUnittest : public ::testing::Test {
public:
    void TestInitSuccess();
    void TestInitMissingBrokers();
    void TestInitMissingTopic();
    void TestSendSuccess();
    void TestStartStop();
    void TestDynamicTopic_Success();
    void TestDynamicTopic_FallbackToStatic();
    void TestDynamicTopic_FromTags();
    void TestPartitionKey_Random();
    void TestPartitionKey_Hash();
    void TestPartitionKey_InvalidPartitionerType();
    void TestPartitionKey_InvalidHashKey();
    void TestPartitionKey_PartialInvalidHashKey();


protected:
    static void SetUpTestSuite();
    static void TearDownTestSuite();

    void SetUp() override;
    void TearDown() override;

private:
    static rd_kafka_mock_cluster_t* sSharedMockCluster;
    static bool sClusterInitialized;
    static std::vector<std::string> sPreCreatedTopics;

    FlusherKafka* mFlusher = nullptr;
    CollectionPipelineContext* mContext = nullptr;
    std::shared_ptr<SourceBuffer> mReusableBuffer;
    Json::Value mBaseConfig;

    Json::Value GetConfigWithTopic(const std::string& topic);
    PipelineEventGroup CreateTestGroup();
    void ForceFlushAndAssert();
    void AssertSendSuccess(int expectedCount);
    void AssertTopicExists(const std::string& expectedTopic);

    void QuickInitAndStart(const std::string& topic);
};


rd_kafka_mock_cluster_t* FlusherKafkaUnittest::sSharedMockCluster = nullptr;
bool FlusherKafkaUnittest::sClusterInitialized = false;
std::vector<std::string> FlusherKafkaUnittest::sPreCreatedTopics;

void FlusherKafkaUnittest::SetUpTestSuite() {
    if (sClusterInitialized)
        return;

    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "test.mock.num.brokers", "3", nullptr, 0);

    char errstr[512];
    rd_kafka_t* temp_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (temp_producer) {
        sSharedMockCluster = rd_kafka_handle_mock_cluster(temp_producer);
        if (sSharedMockCluster) {
            sPreCreatedTopics = {"test_topic",
                                 "test_user_behavior_log",
                                 "logs_nginx_access_log",
                                 "test_%{content.application}",
                                 "logs_%{tag.namespace}"};

            for (const auto& topic : sPreCreatedTopics) {
                rd_kafka_mock_topic_create(sSharedMockCluster, topic.c_str(), 1, 1);
            }
        }
        rd_kafka_destroy(temp_producer);
    }

    sClusterInitialized = true;
}

void FlusherKafkaUnittest::TearDownTestSuite() {
    sClusterInitialized = false;
}

void FlusherKafkaUnittest::SetUp() {
    mContext = new CollectionPipelineContext();
    mContext->SetConfigName("test_config");

    mFlusher = new FlusherKafka();
    mFlusher->SetContext(*mContext);
    mFlusher->CreateMetricsRecordRef(FlusherKafka::sName, "1");

    mReusableBuffer = std::make_shared<SourceBuffer>();

    mBaseConfig["Brokers"] = Json::Value(Json::arrayValue);
    mBaseConfig["Brokers"].append("test.mock.brokers");
    mBaseConfig["Kafka"] = Json::Value(Json::objectValue);
    mBaseConfig["Kafka"]["test.mock.num.brokers"] = "3";
}

void FlusherKafkaUnittest::TearDown() {
    if (mFlusher) {
        if (mFlusher->mIsRunning.load()) {
            rd_kafka_flush(mFlusher->mProducer, 1000);
            mFlusher->Stop(true);
        }
        mFlusher->CommitMetricsRecordRef();
        delete mFlusher;
        mFlusher = nullptr;
    }
    if (mContext) {
        delete mContext;
        mContext = nullptr;
    }
}

Json::Value FlusherKafkaUnittest::GetConfigWithTopic(const std::string& topic) {
    Json::Value config = mBaseConfig;
    config["Topic"] = topic;
    return config;
}

PipelineEventGroup FlusherKafkaUnittest::CreateTestGroup() {
    return PipelineEventGroup(mReusableBuffer);
}

void FlusherKafkaUnittest::ForceFlushAndAssert() {
    rd_kafka_flush(mFlusher->mProducer, 1000);
    rd_kafka_poll(mFlusher->mProducer, 100);
}

void FlusherKafkaUnittest::AssertSendSuccess(int expectedCount) {
    APSARA_TEST_EQUAL(expectedCount, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(expectedCount, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(expectedCount, mFlusher->mSuccessCnt->GetValue());
}

void FlusherKafkaUnittest::AssertTopicExists(const std::string& expectedTopic) {
    APSARA_TEST_TRUE(mFlusher->mTopicSet.find(expectedTopic) != mFlusher->mTopicSet.end());
}

void FlusherKafkaUnittest::QuickInitAndStart(const std::string& topic) {
    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic(topic);
    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());


    rd_kafka_poll(mFlusher->mProducer, 50);
}

void FlusherKafkaUnittest::TestInitSuccess() {
    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic("test_topic");
    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
}

void FlusherKafkaUnittest::TestInitMissingBrokers() {
    Json::Value config;
    Json::Value optionalGoPipeline;
    config["Topic"] = "test_topic";
    APSARA_TEST_FALSE(mFlusher->Init(config, optionalGoPipeline));
}

void FlusherKafkaUnittest::TestInitMissingTopic() {
    Json::Value config;
    Json::Value optionalGoPipeline;
    config["Brokers"] = Json::Value(Json::arrayValue);
    config["Brokers"].append("dummy:9092");
    APSARA_TEST_FALSE(mFlusher->Init(config, optionalGoPipeline));
}

void FlusherKafkaUnittest::TestSendSuccess() {
    QuickInitAndStart("test_topic");

    PipelineEventGroup group = CreateTestGroup();
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));
    ForceFlushAndAssert();
    AssertSendSuccess(1);
}

void FlusherKafkaUnittest::TestStartStop() {
    QuickInitAndStart("test_topic");
    APSARA_TEST_TRUE(mFlusher->mIsRunning.load());

    APSARA_TEST_TRUE(mFlusher->Stop(true));
    APSARA_TEST_FALSE(mFlusher->mIsRunning.load());
}

void FlusherKafkaUnittest::TestDynamicTopic_Success() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_Success");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_Success");

    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic("test_%{content.application}");

    APSARA_TEST_TRUE(tempFlusher.Init(config, optionalGoPipeline));

    PipelineEventGroup group = CreateTestGroup();

    group.SetMetadata(EventGroupMetaKey::SOURCE_ID, StringView("test-source"));
    auto* event = group.AddLogEvent();
    event->SetTimestamp(1234567890);
    event->SetContent(StringView("application"), StringView("user_behavior_log"));
    event->SetContent(StringView("message"), StringView("test message"));

    APSARA_TEST_TRUE(tempFlusher.Send(std::move(group)));

    rd_kafka_flush(tempFlusher.mProducer, 1000);
    rd_kafka_poll(tempFlusher.mProducer, 100);


    APSARA_TEST_TRUE(tempFlusher.mTopicSet.find("test_user_behavior_log") != tempFlusher.mTopicSet.end());

    APSARA_TEST_EQUAL(1, tempFlusher.mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, tempFlusher.mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, tempFlusher.mSuccessCnt->GetValue());

    tempFlusher.Stop(true);
    tempFlusher.CommitMetricsRecordRef();
}

void FlusherKafkaUnittest::TestDynamicTopic_FallbackToStatic() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_FallbackToStatic");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_FallbackToStatic");

    Json::Value optionalGoPipeline;

    Json::Value config = GetConfigWithTopic("test_%{content.application}");

    APSARA_TEST_TRUE(tempFlusher.Init(config, optionalGoPipeline));

    PipelineEventGroup group = CreateTestGroup();

    group.SetMetadata(EventGroupMetaKey::SOURCE_ID, StringView("test-source"));
    auto* event = group.AddLogEvent();
    event->SetTimestamp(1234567890);
    event->SetContent(StringView("message"), StringView("test message"));


    APSARA_TEST_TRUE(tempFlusher.Send(std::move(group)));

    rd_kafka_flush(tempFlusher.mProducer, 1000);
    rd_kafka_poll(tempFlusher.mProducer, 100);


    APSARA_TEST_TRUE(tempFlusher.mTopicSet.find("test_%{content.application}") != tempFlusher.mTopicSet.end());

    APSARA_TEST_EQUAL(1, tempFlusher.mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, tempFlusher.mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, tempFlusher.mSuccessCnt->GetValue());

    tempFlusher.Stop(true);
    tempFlusher.CommitMetricsRecordRef();
}

void FlusherKafkaUnittest::TestDynamicTopic_FromTags() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_FromTags");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_FromTags");

    Json::Value optionalGoPipeline;

    Json::Value config = GetConfigWithTopic("logs_%{tag.namespace}");

    APSARA_TEST_TRUE(tempFlusher.Init(config, optionalGoPipeline));

    PipelineEventGroup group = CreateTestGroup();

    group.SetMetadata(EventGroupMetaKey::SOURCE_ID, StringView("test-source"));
    group.SetTag(StringView("namespace"), StringView("nginx_access_log"));
    auto* event = group.AddLogEvent();
    event->SetTimestamp(1234567890);
    event->SetContent(StringView("message"), StringView("test message"));

    APSARA_TEST_TRUE(tempFlusher.Send(std::move(group)));

    rd_kafka_flush(tempFlusher.mProducer, 1000);
    rd_kafka_poll(tempFlusher.mProducer, 100);


    APSARA_TEST_TRUE(tempFlusher.mTopicSet.find("logs_nginx_access_log") != tempFlusher.mTopicSet.end());

    APSARA_TEST_EQUAL(1, tempFlusher.mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, tempFlusher.mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, tempFlusher.mSuccessCnt->GetValue());

    tempFlusher.Stop(true);
    tempFlusher.CommitMetricsRecordRef();
}

void FlusherKafkaUnittest::TestPartitionKey_Random() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_Random");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_Random");

    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic("test_topic");
    config["PartitionerType"] = "random";

    bool initResult = tempFlusher.Init(config, optionalGoPipeline);
    APSARA_TEST_TRUE(initResult);

    if (initResult) {
        PipelineEventGroup group = CreateTestGroup();
        group.SetMetadata(EventGroupMetaKey::SOURCE_ID, StringView("test-source"));
        auto* event = group.AddLogEvent();
        event->SetTimestamp(1234567890);
        event->SetContent(StringView("message"), StringView("test message for random"));

        APSARA_TEST_TRUE(tempFlusher.Send(std::move(group)));

        rd_kafka_flush(tempFlusher.mProducer, 1000);
        rd_kafka_poll(tempFlusher.mProducer, 100);


        APSARA_TEST_EQUAL(1, tempFlusher.mSendCnt->GetValue());
        APSARA_TEST_EQUAL(1, tempFlusher.mSendDoneCnt->GetValue());
        APSARA_TEST_EQUAL(1, tempFlusher.mSuccessCnt->GetValue());

        tempFlusher.Stop(true);
        tempFlusher.CommitMetricsRecordRef();
    }
}

void FlusherKafkaUnittest::TestPartitionKey_Hash() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_Hash");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_Hash");

    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic("test_topic");
    config["PartitionerType"] = "hash";
    config["HashKeys"] = Json::Value(Json::arrayValue);
    config["HashKeys"].append("content.user_id");
    config["HashKeys"].append("content.session_id");

    bool initResult = tempFlusher.Init(config, optionalGoPipeline);
    APSARA_TEST_TRUE(initResult);

    if (initResult) {
        PipelineEventGroup group = CreateTestGroup();
        group.SetMetadata(EventGroupMetaKey::SOURCE_ID, StringView("test-source"));
        auto* event = group.AddLogEvent();
        event->SetTimestamp(1234567890);
        event->SetContent(StringView("user_id"), StringView("user123"));
        event->SetContent(StringView("session_id"), StringView("session456"));

        APSARA_TEST_TRUE(tempFlusher.Send(std::move(group)));

        rd_kafka_flush(tempFlusher.mProducer, 1000);
        rd_kafka_poll(tempFlusher.mProducer, 100);


        APSARA_TEST_EQUAL(1, tempFlusher.mSendCnt->GetValue());
        APSARA_TEST_EQUAL(1, tempFlusher.mSendDoneCnt->GetValue());
        APSARA_TEST_EQUAL(1, tempFlusher.mSuccessCnt->GetValue());

        tempFlusher.Stop(true);
        tempFlusher.CommitMetricsRecordRef();
    }
}

void FlusherKafkaUnittest::TestPartitionKey_InvalidPartitionerType() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_InvalidPartitionerType");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_InvalidPartitionerType");

    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic("test_topic");
    config["PartitionerType"] = "invalid_type";

    bool initResult = tempFlusher.Init(config, optionalGoPipeline);
    APSARA_TEST_FALSE(initResult);


    tempFlusher.CommitMetricsRecordRef();
}

void FlusherKafkaUnittest::TestPartitionKey_InvalidHashKey() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_InvalidHashKey");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_InvalidHashKey");

    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic("test_topic");
    config["PartitionerType"] = "hash";
    config["HashKeys"] = Json::Value(Json::arrayValue);
    config["HashKeys"].append("content.invalid_key");

    bool initResult = tempFlusher.Init(config, optionalGoPipeline);
    APSARA_TEST_TRUE(initResult);

    if (initResult) {
        PipelineEventGroup group = CreateTestGroup();
        group.SetMetadata(EventGroupMetaKey::SOURCE_ID, StringView("test-source"));
        auto* event = group.AddLogEvent();
        event->SetTimestamp(1234567890);
        event->SetContent(StringView("message"), StringView("test message for invalid hash key"));


        APSARA_TEST_TRUE(tempFlusher.Send(std::move(group)));

        rd_kafka_flush(tempFlusher.mProducer, 1000);
        rd_kafka_poll(tempFlusher.mProducer, 100);


        APSARA_TEST_EQUAL(1, tempFlusher.mSendCnt->GetValue());
        APSARA_TEST_EQUAL(1, tempFlusher.mSendDoneCnt->GetValue());
        APSARA_TEST_EQUAL(1, tempFlusher.mSuccessCnt->GetValue());

        tempFlusher.Stop(true);
        tempFlusher.CommitMetricsRecordRef();
    }
}

void FlusherKafkaUnittest::TestPartitionKey_PartialInvalidHashKey() {
    FlusherKafka tempFlusher;
    CollectionPipelineContext tempContext;
    tempContext.SetConfigName("test_config_PartialInvalidHashKey");
    tempFlusher.SetContext(tempContext);
    tempFlusher.CreateMetricsRecordRef(FlusherKafka::sName, "temp_PartialInvalidHashKey");

    Json::Value optionalGoPipeline;
    Json::Value config = GetConfigWithTopic("test_topic");
    config["PartitionerType"] = "hash";
    config["HashKeys"] = Json::Value(Json::arrayValue);
    config["HashKeys"].append("content.user_id");
    config["HashKeys"].append("invalid_prefix");

    bool initResult = tempFlusher.Init(config, optionalGoPipeline);
    APSARA_TEST_FALSE(initResult);

    tempFlusher.CommitMetricsRecordRef();
}


UNIT_TEST_CASE(FlusherKafkaUnittest, TestInitSuccess)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestInitMissingBrokers)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestInitMissingTopic)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendSuccess)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestStartStop)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestDynamicTopic_Success)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestDynamicTopic_FallbackToStatic)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestDynamicTopic_FromTags)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestPartitionKey_Random)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestPartitionKey_Hash)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestPartitionKey_InvalidPartitionerType)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestPartitionKey_InvalidHashKey)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestPartitionKey_PartialInvalidHashKey)

} // namespace logtail

UNIT_TEST_MAIN
