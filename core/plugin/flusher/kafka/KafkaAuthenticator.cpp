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

#include "KafkaAuthenticator.h"

#include <string>

#include "KafkaConstant.h"
#include "librdkafka/rdkafka.h"
#include "logger/Logger.h"

using namespace std;

namespace logtail {

KafkaAuthenticator::KafkaAuthenticator() = default;

KafkaAuthenticator::~KafkaAuthenticator() {
    delete mSaslConfig;
    delete mTlsConfig;
}

bool KafkaAuthenticator::Init(const Json::Value& authConfig) {
    if (!authConfig.isObject() || authConfig.empty()) {
        return true;
    }

    if (!ValidateConfig(authConfig)) {
        return false;
    }

    mSecurityProtocol = authConfig["SecurityProtocol"].asString();

    if (authConfig.isMember("SASL") && authConfig["SASL"].isObject()) {
        if (!ParseSaslConfig(authConfig["SASL"])) {
            return false;
        }
    }

    if (authConfig.isMember("TLS") && authConfig["TLS"].isObject()) {
        if (!ParseTlsConfig(authConfig["TLS"])) {
            return false;
        }
    }

    return true;
}

bool KafkaAuthenticator::ValidateConfig(const Json::Value& authConfig) {
    if (!authConfig.isMember("SecurityProtocol") || !authConfig["SecurityProtocol"].isString()) {
        LOG_ERROR(sLogger, ("Invalid authentication config", "SecurityProtocol is required and must be a string"));
        return false;
    }

    const std::string securityProtocol = authConfig["SecurityProtocol"].asString();
    if (securityProtocol != "plaintext" && securityProtocol != "ssl" && securityProtocol != "sasl_plaintext"
        && securityProtocol != "sasl_ssl") {
        LOG_ERROR(
            sLogger,
            ("Invalid authentication config", "SecurityProtocol must be plaintext, ssl, sasl_plaintext, or sasl_ssl"));
        return false;
    }

    bool hasSasl = authConfig.isMember("SASL") && authConfig["SASL"].isObject();
    bool hasTls = authConfig.isMember("TLS") && authConfig["TLS"].isObject();

    if ((securityProtocol == "sasl_plaintext" || securityProtocol == "sasl_ssl") && !hasSasl) {
        LOG_ERROR(sLogger, ("Invalid authentication config", "SASL configuration is required for " + securityProtocol));
        return false;
    }

    if ((securityProtocol == "ssl" || securityProtocol == "sasl_ssl") && !hasTls) {
        LOG_ERROR(sLogger, ("Invalid authentication config", "TLS configuration is required for " + securityProtocol));
        return false;
    }

    if ((securityProtocol == "plaintext" || securityProtocol == "ssl") && hasSasl) {
        LOG_ERROR(sLogger,
                  ("Invalid authentication config", "SASL configuration is not allowed for " + securityProtocol));
        return false;
    }

    if ((securityProtocol == "plaintext" || securityProtocol == "sasl_plaintext") && hasTls) {
        LOG_ERROR(sLogger,
                  ("Invalid authentication config", "TLS configuration is not allowed for " + securityProtocol));
        return false;
    }

    for (auto it = authConfig.begin(); it != authConfig.end(); ++it) {
        const std::string& key = it.name();
        if (key != "SecurityProtocol" && key != "SASL" && key != "TLS") {
            LOG_ERROR(sLogger, ("Invalid authentication config", "Unknown authentication type: " + key));
            return false;
        }
    }

    return true;
}


bool KafkaAuthenticator::ParseSaslConfig(const Json::Value& config) {
    mSaslConfig = new SaslConfig();

    if (!config.isMember("Mechanism") || !config["Mechanism"].isString()) {
        LOG_ERROR(sLogger, ("Invalid SASL config", "Mechanism is required and must be a string"));
        delete mSaslConfig;
        mSaslConfig = nullptr;
        return false;
    }

    mSaslConfig->Mechanism = config["Mechanism"].asString();

    const string& mechanism = mSaslConfig->Mechanism;
    if (mechanism != "PLAIN" && mechanism != "SCRAM-SHA-256" && mechanism != "SCRAM-SHA-512" && mechanism != "GSSAPI") {
        LOG_ERROR(sLogger, ("Invalid SASL config", "Mechanism must be PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, or GSSAPI"));
        delete mSaslConfig;
        mSaslConfig = nullptr;
        return false;
    }

    if (mechanism == "GSSAPI") {
        if (!config.isMember("Kerberos") || !config["Kerberos"].isObject()) {
            LOG_ERROR(sLogger, ("Invalid SASL config", "Kerberos configuration is required for GSSAPI mechanism"));
            delete mSaslConfig;
            mSaslConfig = nullptr;
            return false;
        }

        KerberosConfig* kerberosConfig = new KerberosConfig();
        const Json::Value& kerberosConfigJson = config["Kerberos"];

        if (!kerberosConfigJson.isMember("ServiceName") || !kerberosConfigJson["ServiceName"].isString()) {
            LOG_ERROR(sLogger, ("Invalid Kerberos config", "ServiceName is required and must be a string"));
            delete kerberosConfig;
            delete mSaslConfig;
            mSaslConfig = nullptr;
            return false;
        }
        kerberosConfig->ServiceName = kerberosConfigJson["ServiceName"].asString();

        if (!kerberosConfigJson.isMember("Principal") || !kerberosConfigJson["Principal"].isString()) {
            LOG_ERROR(sLogger, ("Invalid Kerberos config", "Principal is required and must be a string"));
            delete kerberosConfig;
            delete mSaslConfig;
            mSaslConfig = nullptr;
            return false;
        }
        kerberosConfig->Principal = kerberosConfigJson["Principal"].asString();

        if (!kerberosConfigJson.isMember("UseKeyTab") || !kerberosConfigJson["UseKeyTab"].isBool()) {
            LOG_ERROR(sLogger, ("Invalid Kerberos config", "UseKeyTab is required and must be a boolean"));
            delete kerberosConfig;
            delete mSaslConfig;
            mSaslConfig = nullptr;
            return false;
        }
        kerberosConfig->UseKeyTab = kerberosConfigJson["UseKeyTab"].asBool();

        if (kerberosConfig->UseKeyTab) {
            if (!kerberosConfigJson.isMember("KeyTabPath") || !kerberosConfigJson["KeyTabPath"].isString()) {
                LOG_ERROR(sLogger, ("Invalid Kerberos config", "KeyTabPath is required when UseKeyTab is true"));
                delete kerberosConfig;
                delete mSaslConfig;
                mSaslConfig = nullptr;
                return false;
            }
            kerberosConfig->KeyTabPath = kerberosConfigJson["KeyTabPath"].asString();
        } else {
            if (!kerberosConfigJson.isMember("Username") || !kerberosConfigJson["Username"].isString()) {
                LOG_ERROR(sLogger, ("Invalid Kerberos config", "Username is required when UseKeyTab is false"));
                delete kerberosConfig;
                delete mSaslConfig;
                mSaslConfig = nullptr;
                return false;
            }
            if (!kerberosConfigJson.isMember("Password") || !kerberosConfigJson["Password"].isString()) {
                LOG_ERROR(sLogger, ("Invalid Kerberos config", "Password is required when UseKeyTab is false"));
                delete kerberosConfig;
                delete mSaslConfig;
                mSaslConfig = nullptr;
                return false;
            }
            kerberosConfig->Username = kerberosConfigJson["Username"].asString();
            kerberosConfig->Password = kerberosConfigJson["Password"].asString();
        }

        if (kerberosConfigJson.isMember("KinitCmd") && kerberosConfigJson["KinitCmd"].isString()) {
            kerberosConfig->KinitCmd = kerberosConfigJson["KinitCmd"].asString();
        }

        mSaslConfig->Kerberos = kerberosConfig;
    } else {
        if (!config.isMember("Username") || !config["Username"].isString()) {
            LOG_ERROR(sLogger, ("Invalid SASL config", "Username is required and must be a string"));
            delete mSaslConfig;
            mSaslConfig = nullptr;
            return false;
        }

        if (!config.isMember("Password") || !config["Password"].isString()) {
            LOG_ERROR(sLogger, ("Invalid SASL config", "Password is required and must be a string"));
            delete mSaslConfig;
            mSaslConfig = nullptr;
            return false;
        }

        mSaslConfig->Username = config["Username"].asString();
        mSaslConfig->Password = config["Password"].asString();
    }

    return true;
}

bool KafkaAuthenticator::ParseTlsConfig(const Json::Value& config) {
    mTlsConfig = new TlsConfig();

    if (config.isMember("CAFile") && config["CAFile"].isString()) {
        mTlsConfig->CAFile = config["CAFile"].asString();
    }

    if (config.isMember("CertFile") && config["CertFile"].isString()) {
        mTlsConfig->CertFile = config["CertFile"].asString();
    }

    if (config.isMember("KeyFile") && config["KeyFile"].isString()) {
        mTlsConfig->KeyFile = config["KeyFile"].asString();
    }

    if (config.isMember("KeyPassword") && config["KeyPassword"].isString()) {
        mTlsConfig->KeyPassword = config["KeyPassword"].asString();
    }

    if (config.isMember("InsecureSkipVerify") && config["InsecureSkipVerify"].isBool()) {
        mTlsConfig->InsecureSkipVerify = config["InsecureSkipVerify"].asBool();
    }

    return true;
}


void KafkaAuthenticator::ApplyConfig(rd_kafka_conf_t* conf) {
    if (!conf) {
        return;
    }

    char errstr[512];
    rd_kafka_conf_set(conf, KAFKA_CONFIG_SECURITY_PROTOCOL.c_str(), mSecurityProtocol.c_str(), errstr, sizeof(errstr));

    if (mTlsConfig != nullptr) {
        if (!mTlsConfig->CAFile.empty()) {
            rd_kafka_conf_set(
                conf, KAFKA_CONFIG_SSL_CA_LOCATION.c_str(), mTlsConfig->CAFile.c_str(), errstr, sizeof(errstr));
        }

        if (!mTlsConfig->CertFile.empty()) {
            rd_kafka_conf_set(conf,
                              KAFKA_CONFIG_SSL_CERTIFICATE_LOCATION.c_str(),
                              mTlsConfig->CertFile.c_str(),
                              errstr,
                              sizeof(errstr));
        }

        if (!mTlsConfig->KeyFile.empty()) {
            rd_kafka_conf_set(
                conf, KAFKA_CONFIG_SSL_KEY_LOCATION.c_str(), mTlsConfig->KeyFile.c_str(), errstr, sizeof(errstr));
        }

        if (!mTlsConfig->KeyPassword.empty()) {
            rd_kafka_conf_set(
                conf, KAFKA_CONFIG_SSL_KEY_PASSWORD.c_str(), mTlsConfig->KeyPassword.c_str(), errstr, sizeof(errstr));
        }

        if (mTlsConfig->InsecureSkipVerify) {
            rd_kafka_conf_set(conf, KAFKA_CONFIG_ENABLE_SSL_CERT_VERIFICATION.c_str(), "false", errstr, sizeof(errstr));
            rd_kafka_conf_set(conf, "ssl.endpoint.identification.algorithm", "none", errstr, sizeof(errstr));
        } else {
            rd_kafka_conf_set(conf, KAFKA_CONFIG_ENABLE_SSL_CERT_VERIFICATION.c_str(), "true", errstr, sizeof(errstr));
        }
    }

    if (mSaslConfig != nullptr) {
        rd_kafka_conf_set(
            conf, KAFKA_CONFIG_SASL_MECHANISM.c_str(), mSaslConfig->Mechanism.c_str(), errstr, sizeof(errstr));

        if (mSaslConfig->Mechanism == "GSSAPI") {
            if (mSaslConfig->Kerberos != nullptr) {
                rd_kafka_conf_set(conf,
                                  KAFKA_CONFIG_SASL_MECHANISM.c_str(),
                                  KAFKA_SASL_MECHANISM_GSSAPI.c_str(),
                                  errstr,
                                  sizeof(errstr));
                rd_kafka_conf_set(conf,
                                  KAFKA_CONFIG_SASL_KERBEROS_SERVICE_NAME.c_str(),
                                  mSaslConfig->Kerberos->ServiceName.c_str(),
                                  errstr,
                                  sizeof(errstr));

                if (!mSaslConfig->Kerberos->Principal.empty()) {
                    rd_kafka_conf_set(conf,
                                      KAFKA_CONFIG_SASL_KERBEROS_PRINCIPAL.c_str(),
                                      mSaslConfig->Kerberos->Principal.c_str(),
                                      errstr,
                                      sizeof(errstr));
                }

                if (mSaslConfig->Kerberos->UseKeyTab) {
                    if (!mSaslConfig->Kerberos->KeyTabPath.empty()) {
                        rd_kafka_conf_set(conf,
                                          KAFKA_CONFIG_SASL_KERBEROS_KEYTAB.c_str(),
                                          mSaslConfig->Kerberos->KeyTabPath.c_str(),
                                          errstr,
                                          sizeof(errstr));
                    }
                } else {
                    rd_kafka_conf_set(conf,
                                      KAFKA_CONFIG_SASL_USERNAME.c_str(),
                                      mSaslConfig->Kerberos->Username.c_str(),
                                      errstr,
                                      sizeof(errstr));
                    rd_kafka_conf_set(conf,
                                      KAFKA_CONFIG_SASL_PASSWORD.c_str(),
                                      mSaslConfig->Kerberos->Password.c_str(),
                                      errstr,
                                      sizeof(errstr));
                }

                if (!mSaslConfig->Kerberos->KinitCmd.empty()) {
                    rd_kafka_conf_set(conf,
                                      "sasl.kerberos.kinit.cmd",
                                      mSaslConfig->Kerberos->KinitCmd.c_str(),
                                      errstr,
                                      sizeof(errstr));
                }
            }
        } else {
            rd_kafka_conf_set(
                conf, KAFKA_CONFIG_SASL_USERNAME.c_str(), mSaslConfig->Username.c_str(), errstr, sizeof(errstr));
            rd_kafka_conf_set(
                conf, KAFKA_CONFIG_SASL_PASSWORD.c_str(), mSaslConfig->Password.c_str(), errstr, sizeof(errstr));
        }
    }
}

bool KafkaAuthenticator::IsAuthenticationEnabled() const {
    return mSaslConfig != nullptr || mTlsConfig != nullptr;
}

} // namespace logtail
