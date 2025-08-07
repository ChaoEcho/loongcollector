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

#pragma once

#include <json/json.h>

#include <memory>
#include <optional>
#include <string>

struct rd_kafka_conf_s;
typedef struct rd_kafka_conf_s rd_kafka_conf_t;

namespace logtail {

struct KerberosConfig {
    std::string ServiceName;
    std::string Principal;
    bool UseKeyTab = false;
    std::string KeyTabPath;
    std::string Username;
    std::string Password;
    std::string KinitCmd;
};

struct SaslConfig {
    std::string Username;
    std::string Password;
    std::string Mechanism;
    KerberosConfig* Kerberos = nullptr;
};

struct TlsConfig {
    std::string CAFile;
    std::string CertFile;
    std::string KeyFile;
    std::string KeyPassword;
    bool InsecureSkipVerify = false;
};

class KafkaAuthenticator {
public:
    KafkaAuthenticator();
    ~KafkaAuthenticator();

    bool Init(const Json::Value& authConfig);
    void ApplyConfig(rd_kafka_conf_t* conf);

    bool IsAuthenticationEnabled() const;
    bool HasSaslConfig() const { return mSaslConfig != nullptr; }
    bool HasTlsConfig() const { return mTlsConfig != nullptr; }

private:
    bool ValidateConfig(const Json::Value& authConfig);
    bool ParseSaslConfig(const Json::Value& config);
    bool ParseTlsConfig(const Json::Value& config);

    std::string mSecurityProtocol;
    SaslConfig* mSaslConfig = nullptr;
    TlsConfig* mTlsConfig = nullptr;
};

} // namespace logtail
