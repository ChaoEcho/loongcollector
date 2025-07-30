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

#include "plugin/flusher/kafka/FlusherKafka.h"

#include <cstring>

#include <sstream>

#include "collection_pipeline/batch/BatchedEvents.h"
#include "collection_pipeline/queue/SenderQueueManager.h"
#include "common/ParamExtractor.h"
#include "logger/Logger.h"
#include "models/LogEvent.h"
#include "models/MetricEvent.h"
#include "models/RawEvent.h"
#include "models/SpanEvent.h"
#include "monitor/AlarmManager.h"
#include "monitor/metric_constants/MetricConstants.h"
#include "plugin/flusher/kafka/TopicFormatParser.h"

using namespace std;

namespace logtail {

const string FlusherKafka::sName = "flusher_kafka";

FlusherKafka::FlusherKafka()
    : mClientID(KAFKA_DEFAULT_CLIENT_ID),
      mTimeoutMs(KAFKA_DEFAULT_TIMEOUT_MS),
      mRetries(KAFKA_DEFAULT_RETRIES),
      mBatchNumMessages(KAFKA_DEFAULT_BATCH_NUM_MESSAGES),
      mLingerMs(KAFKA_DEFAULT_LINGER_MS),
      mProducer(nullptr),
      mKafkaTopic(nullptr),
      mKafkaConf(nullptr),
      mTopicConf(nullptr),
      mIsRunning(false) {
}

FlusherKafka::~FlusherKafka() {
    Stop(true);
    DestroyKafkaResources();
}

bool FlusherKafka::Init(const Json::Value& config, Json::Value& optionalGoPipeline) {
    string errorMsg;

    if (!GetMandatoryListParam<string>(config, "Brokers", mBrokers, errorMsg)) {
        PARAM_ERROR_RETURN(mContext->GetLogger(),
                           mContext->GetAlarm(),
                           errorMsg,
                           sName,
                           mContext->GetConfigName(),
                           mContext->GetProjectName(),
                           mContext->GetLogstoreName(),
                           mContext->GetRegion());
    }

    if (!GetMandatoryStringParam(config, "Topic", mTopic, errorMsg)) {
        PARAM_ERROR_RETURN(mContext->GetLogger(),
                           mContext->GetAlarm(),
                           errorMsg,
                           sName,
                           mContext->GetConfigName(),
                           mContext->GetProjectName(),
                           mContext->GetLogstoreName(),
                           mContext->GetRegion());
    }

    GetOptionalStringParam(config, "ClientID", mClientID, errorMsg);
    GetOptionalUIntParam(config, "TimeoutMs", mTimeoutMs, errorMsg);
    GetOptionalUIntParam(config, "Retries", mRetries, errorMsg);
    GetOptionalUIntParam(config, "BatchNumMessages", mBatchNumMessages, errorMsg);
    GetOptionalUIntParam(config, "LingerMs", mLingerMs, errorMsg);
    GetOptionalStringParam(config, "PartitionerType", mPartitionerType, errorMsg);
    if (mPartitionerType.empty()) {
        mPartitionerType = "random";
    }
    if (!GetOptionalListParam<string>(config, "HashKeys", mHashKeys, errorMsg)) {
        mHashKeys.clear();
    }

    if (mPartitionerType != "random" && mPartitionerType != "roundrobin" && mPartitionerType != "hash") {
        PARAM_ERROR_RETURN(mContext->GetLogger(),
                           mContext->GetAlarm(),
                           "invalid PartitionerType, must be one of: random, roundrobin, hash",
                           sName,
                           mContext->GetConfigName(),
                           mContext->GetProjectName(),
                           mContext->GetLogstoreName(),
                           mContext->GetRegion());
    }

    if (mPartitionerType == "hash") {
        for (const auto& key : mHashKeys) {
            if (key.find("content.") != 0) {
                PARAM_ERROR_RETURN(mContext->GetLogger(),
                                   mContext->GetAlarm(),
                                   "invalid HashKeys format, must start with 'content.': " + key,
                                   sName,
                                   mContext->GetConfigName(),
                                   mContext->GetProjectName(),
                                   mContext->GetLogstoreName(),
                                   mContext->GetRegion());
            }
        }
    }

    if (!InitKafkaProducer()) {
        return false;
    }

    mSerializer = make_unique<JsonEventGroupSerializer>(this);
    mTopicParser = make_unique<TopicFormatParser>();

    if (!mTopicParser->Init(mTopic)) {
        LOG_ERROR(mContext->GetLogger(), ("invalid topic format string", mTopic));
        return false;
    }

    GenerateQueueKey(mTopic);
    SenderQueueManager::GetInstance()->CreateQueue(mQueueKey, mPluginID, *mContext);

    mSendCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_OUT_EVENT_GROUPS_TOTAL);
    mSuccessCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_SUCCESS_TOTAL);
    mSendDoneCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_SEND_DONE_TOTAL);
    mDiscardCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_DISCARD_TOTAL);
    mNetworkErrorCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_NETWORK_ERROR_TOTAL);
    mServerErrorCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_SERVER_ERROR_TOTAL);
    mUnauthErrorCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_UNAUTH_ERROR_TOTAL);
    mParamsErrorCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_PARAMS_ERROR_TOTAL);
    mOtherErrorCnt = GetMetricsRecordRef().CreateCounter(METRIC_PLUGIN_FLUSHER_OTHER_ERROR_TOTAL);

    LOG_INFO(mContext->GetLogger(),
             ("FlusherKafka initialized successfully", "")("topic", mTopic)("brokers", mBrokers.size()));

    return true;
}

bool FlusherKafka::Start() {
    if (!Flusher::Start()) {
        return false;
    }

    try {
        mIsRunning = true;
        mPollThread = std::thread([this]() {
            LOG_INFO(mContext->GetLogger(), ("Kafka poll thread started", ""));
            while (mIsRunning.load(std::memory_order_relaxed)) {
                {
                    std::lock_guard<std::mutex> lock(mProducerMutex);
                    if (mProducer) {
                        rd_kafka_poll(mProducer, KAFKA_POLL_INTERVAL_MS);
                    }
                }
            }
            LOG_INFO(mContext->GetLogger(), ("Kafka poll thread stopped", ""));
        });
        LOG_INFO(mContext->GetLogger(), ("Kafka poll thread created successfully", ""));
        return true;
    } catch (const std::exception& e) {
        mIsRunning = false;
        LOG_ERROR(mContext->GetLogger(),
                  ("Failed to create Kafka poll thread", e.what())("action", "Kafka flusher cannot start properly"));
        return false;
    }
}

bool FlusherKafka::Stop(bool isPipelineRemoving) {
    if (mIsRunning.exchange(false)) {
        LOG_INFO(mContext->GetLogger(), ("stopping kafka poll thread", ""));
        if (mPollThread.joinable()) {
            mPollThread.join();
        }
    }

    {
        std::lock_guard<std::mutex> lock(mProducerMutex);
        if (mProducer) {
            LOG_INFO(mContext->GetLogger(), ("kafka flushing messages before shutdown", "")("timeout_ms", mTimeoutMs));
            rd_kafka_flush(mProducer, mTimeoutMs);
        }
    }

    return Flusher::Stop(isPipelineRemoving);
}

bool FlusherKafka::Send(PipelineEventGroup&& g) {
    return SerializeAndSend(std::move(g));
}

bool FlusherKafka::Flush(size_t key) {
    std::lock_guard<std::mutex> lock(mProducerMutex);
    if (mProducer) {
        rd_kafka_resp_err_t result = rd_kafka_flush(mProducer, KAFKA_FLUSH_TIMEOUT_MS);
        if (result != RD_KAFKA_RESP_ERR_NO_ERROR) {
            LOG_WARNING(mContext->GetLogger(), ("failed to flush kafka producer", KafkaUtil::GetErrorString(result)));
            return false;
        }
    }
    return true;
}

bool FlusherKafka::FlushAll() {
    return Flush(0);
}

bool FlusherKafka::InitKafkaProducer() {
    char errstr[512];

    mKafkaConf = rd_kafka_conf_new();
    if (!mKafkaConf) {
        LOG_ERROR(mContext->GetLogger(), ("failed to create kafka configuration", ""));
        return false;
    }

    std::string brokersStr = KafkaUtil::BrokersToString(mBrokers);
    if (brokersStr.empty()) {
        LOG_ERROR(mContext->GetLogger(), ("empty brokers list", ""));
        return false;
    }

    if (rd_kafka_conf_set(
            mKafkaConf, KAFKA_CONFIG_BOOTSTRAP_SERVERS.c_str(), brokersStr.c_str(), errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
        LOG_ERROR(mContext->GetLogger(), ("failed to set bootstrap servers", errstr));
        return false;
    }

    if (rd_kafka_conf_set(mKafkaConf, KAFKA_CONFIG_CLIENT_ID.c_str(), mClientID.c_str(), errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
        LOG_ERROR(mContext->GetLogger(), ("failed to set client id", errstr));
        return false;
    }

    rd_kafka_conf_set_dr_msg_cb(mKafkaConf, DeliveryReportCallback);
    rd_kafka_conf_set_opaque(mKafkaConf, this);

    if (mPartitionerType == "random") {
        if (rd_kafka_conf_set(mKafkaConf, "partitioner", "random", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            LOG_ERROR(mContext->GetLogger(), ("failed to set partitioner to random", errstr));
            return false;
        }
    }

    std::string retriesStr = std::to_string(mRetries);
    if (rd_kafka_conf_set(mKafkaConf, KAFKA_CONFIG_RETRIES.c_str(), retriesStr.c_str(), errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
        LOG_ERROR(mContext->GetLogger(), ("failed to set retries", errstr));
        return false;
    }

    std::string batchNumStr = std::to_string(mBatchNumMessages);
    if (rd_kafka_conf_set(
            mKafkaConf, KAFKA_CONFIG_BATCH_NUM_MESSAGES.c_str(), batchNumStr.c_str(), errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
        LOG_ERROR(mContext->GetLogger(), ("failed to set batch.num.messages", errstr));
        return false;
    }

    std::string lingerMsStr = std::to_string(mLingerMs);
    if (rd_kafka_conf_set(mKafkaConf, KAFKA_CONFIG_LINGER_MS.c_str(), lingerMsStr.c_str(), errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
        LOG_ERROR(mContext->GetLogger(), ("failed to set linger.ms", errstr));
        return false;
    }

    mProducer = rd_kafka_new(RD_KAFKA_PRODUCER, mKafkaConf, errstr, sizeof(errstr));
    if (!mProducer) {
        LOG_ERROR(mContext->GetLogger(), ("failed to create kafka producer", errstr));
        return false;
    }

    mKafkaConf = nullptr;

    mTopicConf = rd_kafka_topic_conf_new();
    if (!mTopicConf) {
        LOG_ERROR(mContext->GetLogger(), ("failed to create topic configuration", ""));
        return false;
    }

    mKafkaTopic = rd_kafka_topic_new(mProducer, mTopic.c_str(), mTopicConf);
    if (!mKafkaTopic) {
        LOG_ERROR(mContext->GetLogger(), ("failed to create kafka topic", mTopic));
        return false;
    }

    mTopicConf = nullptr;

    LOG_INFO(mContext->GetLogger(),
             ("kafka producer initialized successfully", "")("topic", mTopic)("brokers", brokersStr));

    return true;
}

bool FlusherKafka::SerializeAndSend(PipelineEventGroup&& group) {
    std::lock_guard<std::mutex> lock(mProducerMutex);

    if (!mProducer) {
        LOG_ERROR(mContext->GetLogger(), ("kafka producer not initialized", ""));
        return false;
    }

    auto& events = group.MutableEvents();
    if (events.empty()) {
        return true;
    }

    GroupTags groupTags = group.GetTags();

    // Batch processing implementation - optimized for reduced function calls
    std::vector<std::string> topicStrings;
    std::vector<std::string> partitionKeys;
    std::vector<std::string> serializedDataList;

    topicStrings.reserve(events.size());
    partitionKeys.reserve(events.size());
    serializedDataList.reserve(events.size());

    // Pre-process all events
    size_t successfulMessages = 0;
    for (const auto& event : events) {
        // Determine dynamic topic
        std::string topic = mTopic;
        if (mTopicParser && mTopicParser->IsDynamic()) {
            bool success = mTopicParser->FormatTopic(event, topic, groupTags);
            if (!success) {
                topic = mTopic;
                LOG_ERROR(mContext->GetLogger(), ("Failed to format dynamic topic from template", mTopic));
            }
        }

        // Generate partition key
        std::string partitionKey = BuildPartitionKey(event);

        // Create temporary BatchedEvents for single event serialization
        BatchedEvents singleEventBatch;
        singleEventBatch.mEvents.emplace_back(event.Copy());
        singleEventBatch.mTags = group.GetSizedTags();
        singleEventBatch.mSourceBuffers.emplace_back(group.GetSourceBuffer());
        singleEventBatch.mExactlyOnceCheckpoint = group.GetExactlyOnceCheckpoint();

        std::string serializedData;
        std::string errorMsg;
        if (!mSerializer->DoSerialize(std::move(singleEventBatch), serializedData, errorMsg)) {
            LOG_ERROR(mContext->GetLogger(),
                      ("failed to serialize event", errorMsg)("topic", topic)("action", "discard data"));
            mContext->GetAlarm().SendAlarm(SERIALIZE_FAIL_ALARM,
                                           "failed to serialize event: " + errorMsg + "\taction: discard data",
                                           mContext->GetRegion(),
                                           mContext->GetProjectName(),
                                           mContext->GetConfigName(),
                                           mContext->GetLogstoreName());
            mDiscardCnt->Add(1);
            continue;
        }

        topicStrings.emplace_back(std::move(topic));
        partitionKeys.emplace_back(std::move(partitionKey));
        serializedDataList.emplace_back(std::move(serializedData));
        successfulMessages++;
    }

    if (successfulMessages == 0) {
        return false;
    }

    mSendCnt->Add(successfulMessages);

    // Batch send with optimized batch processing
    return SendBatchOptimized(topicStrings, partitionKeys, serializedDataList);
}

bool FlusherKafka::SendBatchOptimized(const std::vector<std::string>& topics,
                                      const std::vector<std::string>& partitionKeys,
                                      std::vector<std::string>& serializedDataList) {
    const int maxRetries = 3;
    const int backoffDelays[] = {100, 500, 1000}; // milliseconds

    size_t totalMessages = topics.size();
    size_t sentMessages = 0;

    for (size_t i = 0; i < totalMessages; ++i) {
        rd_kafka_topic_t* topicHandle = GetOrCreateTopicHandle(topics[i]);
        if (!topicHandle) {
            continue;
        }

        const std::string& data = serializedDataList[i];
        const std::string& key = partitionKeys[i];

        bool sent = false;
        for (int attempt = 0; attempt <= maxRetries && !sent; ++attempt) {
            char* payload = static_cast<char*>(malloc(data.size()));
            if (!payload) {
                LOG_ERROR(mContext->GetLogger(), ("failed to allocate memory for kafka message", "out of memory"));
                mOtherErrorCnt->Add(1);
                break;
            }
            std::memcpy(payload, data.data(), data.size());

            int result = rd_kafka_produce(topicHandle,
                                          RD_KAFKA_PARTITION_UA,
                                          RD_KAFKA_MSG_F_FREE,
                                          payload,
                                          data.size(),
                                          key.empty() ? nullptr : key.c_str(),
                                          key.size(),
                                          nullptr);

            if (result == 0) {
                sentMessages++;
                sent = true;
                continue;
            }

            free(payload); // Free memory if produce fails
            rd_kafka_resp_err_t err = rd_kafka_last_error();

            if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && attempt < maxRetries) {
                rd_kafka_poll(mProducer, backoffDelays[attempt]);
                LOG_WARNING(mContext->GetLogger(),
                            ("kafka queue full, retrying",
                             rd_kafka_err2str(err))("attempt", attempt + 1)("backoff_ms", backoffDelays[attempt]));
                continue;
            }

            HandleKafkaError(err);
            break;
        }
    }

    return sentMessages > 0;
}

void FlusherKafka::HandleKafkaError(rd_kafka_resp_err_t err) {
    const std::string errStr = rd_kafka_err2str(err);
    LOG_ERROR(mContext->GetLogger(), ("kafka error occurred", errStr));

    switch (err) {
        case RD_KAFKA_RESP_ERR__AUTHENTICATION:
        case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
        case RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED:
            mUnauthErrorCnt->Add(1);
            break;
        case RD_KAFKA_RESP_ERR__TRANSPORT:
        case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
        case RD_KAFKA_RESP_ERR__DESTROY:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            mNetworkErrorCnt->Add(1);
            break;
        case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
        case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
            mServerErrorCnt->Add(1);
            break;
        case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
        case RD_KAFKA_RESP_ERR__INVALID_ARG:
            mParamsErrorCnt->Add(1);
            break;
        default:
            mOtherErrorCnt->Add(1);
            break;
    }

    mContext->GetAlarm().SendAlarm(SEND_DATA_FAIL_ALARM,
                                   "Kafka error: " + errStr,
                                   mContext->GetRegion(),
                                   mContext->GetProjectName(),
                                   mContext->GetConfigName(),
                                   mTopic);
}

rd_kafka_topic_t* FlusherKafka::GetOrCreateTopicHandle(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mTopicHandlesMutex);

    auto it = mTopicHandles.find(topic);
    if (it != mTopicHandles.end()) {
        return it->second;
    }

    rd_kafka_topic_t* topicHandle = rd_kafka_topic_new(mProducer, topic.c_str(), mTopicConf);
    if (!topicHandle) {
        LOG_ERROR(mContext->GetLogger(),
                  ("failed to create kafka topic handle", "")("topic", topic)("error",
                                                                              rd_kafka_err2str(rd_kafka_last_error())));
        return nullptr;
    }

    mTopicHandles[topic] = topicHandle;
    return topicHandle;
}

void FlusherKafka::DestroyKafkaResources() {
    {
        std::lock_guard<std::mutex> lock(mTopicHandlesMutex);
        for (auto& entry : mTopicHandles) {
            rd_kafka_topic_destroy(entry.second);
        }
        mTopicHandles.clear();
    }

    if (mKafkaTopic) {
        rd_kafka_topic_destroy(mKafkaTopic);
        mKafkaTopic = nullptr;
    }

    if (mProducer) {
        rd_kafka_destroy(mProducer);
        mProducer = nullptr;
    }

    if (mTopicConf) {
        rd_kafka_topic_conf_destroy(mTopicConf);
        mTopicConf = nullptr;
    }

    if (mKafkaConf) {
        rd_kafka_conf_destroy(mKafkaConf);
        mKafkaConf = nullptr;
    }
}

void FlusherKafka::DeliveryReportCallback(rd_kafka_t* rk, const rd_kafka_message_t* message, void* opaque) {
    auto* flusher = static_cast<FlusherKafka*>(opaque);
    if (!flusher) {
        return;
    }
    flusher->mSendDoneCnt->Add(1);

    if (message->err) {
        const std::string errStr = rd_kafka_err2str(message->err);
        LOG_ERROR(flusher->mContext->GetLogger(),
                  ("kafka message delivery failed", errStr)("topic", rd_kafka_topic_name(message->rkt))(
                      "partition", message->partition)("offset", message->offset));

        flusher->HandleKafkaError(message->err);
    } else {
        flusher->mSuccessCnt->Add(1);
        LOG_DEBUG(flusher->mContext->GetLogger(),
                  ("kafka message delivered successfully", "")("topic", rd_kafka_topic_name(message->rkt))(
                      "partition", message->partition)("offset", message->offset));
    }
}

std::string FlusherKafka::BuildPartitionKey(const PipelineEventPtr& event) const {
    if (mPartitionerType != "hash" || mHashKeys.empty()) {
        return "";
    }

    std::vector<std::string> values;
    for (const auto& key : mHashKeys) {
        // Remove "content." prefix
        std::string fieldName = key.substr(8);
        std::string value;

        switch (event->GetType()) {
            case PipelineEvent::Type::LOG: {
                const LogEvent& logEvent = event.Cast<LogEvent>();
                auto valueOpt = logEvent.GetContent(fieldName);
                if (valueOpt != gEmptyStringView) {
                    values.push_back(valueOpt.to_string());
                }
                break;
            }
            default:
                LOG_ERROR(mContext->GetLogger(),
                          ("unsupported event type", PipelineEventTypeToString(event->GetType()).c_str()));
                break;
        }
    }

    if (values.empty()) {
        return "";
    }

    std::ostringstream oss;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            oss << "###";
        }
        oss << values[i];
    }
    return oss.str();
}

} // namespace logtail
