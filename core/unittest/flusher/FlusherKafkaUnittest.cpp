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

#include <functional>
#include <memory>
#include <string>

#include "collection_pipeline/CollectionPipelineContext.h"
#include "common/memory/SourceBuffer.h"
#include "models/LogEvent.h"
#include "models/PipelineEventGroup.h"
#include "plugin/flusher/kafka/FlusherKafka.h"
#include "unittest/Unittest.h"
#include "unittest/flusher/MockKafkaProducer.h"

using namespace std;

namespace logtail {

Json::Value CreateKafkaTestConfig(const std::string& topic) {
    Json::Value config;
    config["Brokers"] = Json::Value(Json::arrayValue);
    config["Brokers"].append("test.mock.brokers");
    config["Topic"] = topic;
    config["Kafka"] = Json::Value(Json::objectValue);
    config["Kafka"]["test.mock.num.brokers"] = "3";
    return config;
}

class FlusherKafkaUnittest : public ::testing::Test {
public:
    void TestInitSuccess();
    void TestInitMissingBrokers();
    void TestInitMissingTopic();
    void TestSendSuccess();
    void TestSendFailure();
    void TestStartStop();
    void TestFlush();
    void TestInitProducerFailure();
    void TestSendNetworkError();
    void TestSendAuthError();
    void TestSendServerError();
    void TestSendParamsError();
    void TestSendQueueFullError();
    void TestFlushFailure();
    void TestDynamicTopic_Success();
    void TestDynamicTopic_FallbackToStatic();
    void TestDynamicTopic_FromTags();
    void TestPartitionKey_Random();
    void TestPartitionKey_Hash();
    void TestPartitionKey_InvalidPartitionerType();
    void TestPartitionKey_InvalidHashKey();
    void TestPartitionKey_PartialInvalidHashKey();

protected:
    void SetUp();
    void TearDown();

private:
    FlusherKafka* mFlusher = nullptr;
    CollectionPipelineContext* mContext = nullptr;

    MockKafkaProducer* mMockProducer = nullptr;
    string mTopic = "test_topic";
};

void FlusherKafkaUnittest::SetUp() {
    mContext = new CollectionPipelineContext();
    mContext->SetConfigName("test_config");

    mFlusher = new FlusherKafka();
    auto mockProducer = std::make_unique<MockKafkaProducer>();
    mMockProducer = mockProducer.get();

    mFlusher->SetProducerForTest(std::move(mockProducer));
    mFlusher->SetContext(*mContext);
    mFlusher->CreateMetricsRecordRef(FlusherKafka::sName, "1");
}

void FlusherKafkaUnittest::TearDown() {
    if (mFlusher) {
        mFlusher->Stop(true);
        mFlusher->CommitMetricsRecordRef();
        delete mFlusher;
        mFlusher = nullptr;
    }
    if (mContext) {
        delete mContext;
        mContext = nullptr;
    }
}

void FlusherKafkaUnittest::TestInitSuccess() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);

    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_EQUAL(mTopic, mFlusher->mKafkaConfig.Topic);
    APSARA_TEST_EQUAL(1, mFlusher->mKafkaConfig.Brokers.size());
    APSARA_TEST_EQUAL("test.mock.brokers", mFlusher->mKafkaConfig.Brokers[0]);
}

void FlusherKafkaUnittest::TestInitMissingBrokers() {
    Json::Value config;
    Json::Value optionalGoPipeline;
    config["Topic"] = mTopic;
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
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);

    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());

    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup group(sourceBuffer);
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
}

void FlusherKafkaUnittest::TestSendFailure() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mFlusher->Init(config, optionalGoPipeline);
    mFlusher->Start();

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));


    mMockProducer->SetAutoComplete(false);
    mFlusher->Send(std::move(group));
    mMockProducer->CompleteLastRequest(false, {KafkaProducer::ErrorType::OTHER_ERROR, "mock general error", -1});


    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(0, mFlusher->mSuccessCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mOtherErrorCnt->GetValue());
}

void FlusherKafkaUnittest::TestStartStop() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);

    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());
    APSARA_TEST_TRUE(mFlusher->Stop(true));
}

void FlusherKafkaUnittest::TestFlush() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);

    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Flush(0));
    APSARA_TEST_TRUE(mFlusher->FlushAll());
}

void FlusherKafkaUnittest::TestInitProducerFailure() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mMockProducer->SetInitSuccess(false);

    APSARA_TEST_FALSE(mFlusher->Init(config, optionalGoPipeline));
}

void FlusherKafkaUnittest::TestSendNetworkError() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mFlusher->Init(config, optionalGoPipeline);
    mFlusher->Start();

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    mMockProducer->SetAutoComplete(false);
    mFlusher->Send(std::move(group));
    mMockProducer->CompleteLastRequest(false, {KafkaProducer::ErrorType::NETWORK_ERROR, "mock network error", 0});

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(0, mFlusher->mSuccessCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mNetworkErrorCnt->GetValue());
}

void FlusherKafkaUnittest::TestSendAuthError() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mFlusher->Init(config, optionalGoPipeline);
    mFlusher->Start();

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    mMockProducer->SetAutoComplete(false);
    mFlusher->Send(std::move(group));
    mMockProducer->CompleteLastRequest(false, {KafkaProducer::ErrorType::AUTH_ERROR, "mock auth error", 0});

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(0, mFlusher->mSuccessCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mUnauthErrorCnt->GetValue());
}

void FlusherKafkaUnittest::TestSendServerError() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mFlusher->Init(config, optionalGoPipeline);
    mFlusher->Start();

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    mMockProducer->SetAutoComplete(false);
    mFlusher->Send(std::move(group));
    mMockProducer->CompleteLastRequest(false, {KafkaProducer::ErrorType::SERVER_ERROR, "mock server error", 0});

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(0, mFlusher->mSuccessCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mServerErrorCnt->GetValue());
}

void FlusherKafkaUnittest::TestSendParamsError() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mFlusher->Init(config, optionalGoPipeline);
    mFlusher->Start();

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    mMockProducer->SetAutoComplete(false);
    mFlusher->Send(std::move(group));
    mMockProducer->CompleteLastRequest(false, {KafkaProducer::ErrorType::PARAMS_ERROR, "mock params error", 0});

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(0, mFlusher->mSuccessCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mParamsErrorCnt->GetValue());
}

void FlusherKafkaUnittest::TestSendQueueFullError() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mFlusher->Init(config, optionalGoPipeline);
    mFlusher->Start();

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    mMockProducer->SetAutoComplete(false);
    mFlusher->Send(std::move(group));
    mMockProducer->CompleteLastRequest(false, {KafkaProducer::ErrorType::QUEUE_FULL, "mock queue full error", 0});

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(0, mFlusher->mSuccessCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mDiscardCnt->GetValue());
}

void FlusherKafkaUnittest::TestFlushFailure() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    mFlusher->Init(config, optionalGoPipeline);
    mMockProducer->SetFlushSuccess(false);

    APSARA_TEST_FALSE(mFlusher->Flush(0));
    APSARA_TEST_TRUE(mMockProducer->IsFlushCalled());
}

void FlusherKafkaUnittest::TestDynamicTopic_Success() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig("test_%{content.application}");
    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());

    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup group(sourceBuffer);
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("application"), StringView("user_behavior_log"));

    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));
    APSARA_TEST_TRUE(mFlusher->mTopicSet.find("test_user_behavior_log") != mFlusher->mTopicSet.end());
    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSuccessCnt->GetValue());
}

void FlusherKafkaUnittest::TestDynamicTopic_FallbackToStatic() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig("test_%{content.application}");
    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));

    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));
    APSARA_TEST_TRUE(mFlusher->mTopicSet.find("test_%{content.application}") != mFlusher->mTopicSet.end());
    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSuccessCnt->GetValue());
}

void FlusherKafkaUnittest::TestDynamicTopic_FromTags() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig("logs_%{tag.namespace}");
    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());

    PipelineEventGroup group(std::make_shared<SourceBuffer>());
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));
    group.SetTag(StringView("namespace"), StringView("nginx_access_log"));

    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));
    APSARA_TEST_TRUE(mFlusher->mTopicSet.find("logs_nginx_access_log") != mFlusher->mTopicSet.end());
    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSuccessCnt->GetValue());
}

void FlusherKafkaUnittest::TestPartitionKey_Random() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    config["PartitionerType"] = "random";

    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());

    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup group(sourceBuffer);
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("key"), StringView("value"));
    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));
    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSuccessCnt->GetValue());
}

void FlusherKafkaUnittest::TestPartitionKey_Hash() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    config["PartitionerType"] = "hash";
    config["HashKeys"] = Json::Value(Json::arrayValue);
    config["HashKeys"].append("content.user_id");
    config["HashKeys"].append("content.session_id");

    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());

    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup group(sourceBuffer);
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("user_id"), StringView("user123"));
    event->SetContent(StringView("session_id"), StringView("session456"));
    event->SetContent(StringView("key"), StringView("value"));

    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSuccessCnt->GetValue());
}

void FlusherKafkaUnittest::TestPartitionKey_InvalidPartitionerType() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    config["PartitionerType"] = "invalid_type";

    APSARA_TEST_FALSE(mFlusher->Init(config, optionalGoPipeline));
}

void FlusherKafkaUnittest::TestPartitionKey_InvalidHashKey() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    config["PartitionerType"] = "hash";
    config["HashKeys"] = Json::Value(Json::arrayValue);
    config["HashKeys"].append("content.invalid_key");

    APSARA_TEST_TRUE(mFlusher->Init(config, optionalGoPipeline));
    APSARA_TEST_TRUE(mFlusher->Start());

    auto sourceBuffer = std::make_shared<SourceBuffer>();
    PipelineEventGroup group(sourceBuffer);
    auto* event = group.AddLogEvent();
    event->SetContent(StringView("user_id"), StringView("user123"));
    event->SetContent(StringView("session_id"), StringView("session456"));
    event->SetContent(StringView("key"), StringView("value"));

    APSARA_TEST_TRUE(mFlusher->Send(std::move(group)));

    APSARA_TEST_EQUAL(1, mFlusher->mSendCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSendDoneCnt->GetValue());
    APSARA_TEST_EQUAL(1, mFlusher->mSuccessCnt->GetValue());
}

void FlusherKafkaUnittest::TestPartitionKey_PartialInvalidHashKey() {
    Json::Value optionalGoPipeline;
    Json::Value config = CreateKafkaTestConfig(mTopic);
    config["PartitionerType"] = "hash";
    config["HashKeys"] = Json::Value(Json::arrayValue);
    config["HashKeys"].append("content.user_id");
    config["HashKeys"].append("invalid_prefix");

    APSARA_TEST_FALSE(mFlusher->Init(config, optionalGoPipeline));
}


UNIT_TEST_CASE(FlusherKafkaUnittest, TestInitSuccess)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestInitMissingBrokers)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestInitMissingTopic)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendSuccess)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendFailure)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestStartStop)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestFlush)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestInitProducerFailure)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendNetworkError)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendAuthError)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendServerError)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendParamsError)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestSendQueueFullError)
UNIT_TEST_CASE(FlusherKafkaUnittest, TestFlushFailure)
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
