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

#include <memory>

#include "plugin/flusher/kafka/KafkaAuthenticator.h"
#include "unittest/Unittest.h"

using namespace std;

namespace logtail {

class KafkaAuthenticatorUnittest : public ::testing::Test {
public:
    void TestInitNone();
    void TestInitPlaintext();
    void TestInitSSL();
    void TestInitSASLPlaintext();
    void TestInitSASLSSL();
    void TestInitSASLWithKerberosKeytab();
    void TestInitSASLWithKerberosPassword();
    void TestConfigValidation();
    void TestInvalidConfig();

protected:
    void SetUp() override { mAuthenticator = make_unique<KafkaAuthenticator>(); }
    void TearDown() override { mAuthenticator.reset(); }

private:
    unique_ptr<KafkaAuthenticator> mAuthenticator;
};

void KafkaAuthenticatorUnittest::TestInitNone() {
    Json::Value emptyConfig;
    APSARA_TEST_TRUE(mAuthenticator->Init(emptyConfig));
    APSARA_TEST_FALSE(mAuthenticator->IsAuthenticationEnabled());
    APSARA_TEST_FALSE(mAuthenticator->HasSaslConfig());
    APSARA_TEST_FALSE(mAuthenticator->HasTlsConfig());
}

void KafkaAuthenticatorUnittest::TestInitPlaintext() {
    Json::Value authConfig;
    authConfig["SecurityProtocol"] = "plaintext";

    APSARA_TEST_TRUE(mAuthenticator->Init(authConfig));
    APSARA_TEST_FALSE(mAuthenticator->IsAuthenticationEnabled());
    APSARA_TEST_FALSE(mAuthenticator->HasSaslConfig());
    APSARA_TEST_FALSE(mAuthenticator->HasTlsConfig());
}

void KafkaAuthenticatorUnittest::TestInitSSL() {
    Json::Value authConfig;
    authConfig["SecurityProtocol"] = "ssl";
    authConfig["TLS"]["CAFile"] = "/path/to/ca.pem";
    authConfig["TLS"]["CertFile"] = "/path/to/cert.pem";
    authConfig["TLS"]["KeyFile"] = "/path/to/key.pem";
    authConfig["TLS"]["InsecureSkipVerify"] = true;

    APSARA_TEST_TRUE(mAuthenticator->Init(authConfig));
    APSARA_TEST_TRUE(mAuthenticator->IsAuthenticationEnabled());
    APSARA_TEST_FALSE(mAuthenticator->HasSaslConfig());
    APSARA_TEST_TRUE(mAuthenticator->HasTlsConfig());
}

void KafkaAuthenticatorUnittest::TestInitSASLPlaintext() {
    Json::Value authConfig;
    authConfig["SecurityProtocol"] = "sasl_plaintext";
    authConfig["SASL"]["Username"] = "testuser";
    authConfig["SASL"]["Password"] = "testpass";
    authConfig["SASL"]["Mechanism"] = "SCRAM-SHA-512";

    APSARA_TEST_TRUE(mAuthenticator->Init(authConfig));
    APSARA_TEST_TRUE(mAuthenticator->IsAuthenticationEnabled());
    APSARA_TEST_TRUE(mAuthenticator->HasSaslConfig());
    APSARA_TEST_FALSE(mAuthenticator->HasTlsConfig());
}

void KafkaAuthenticatorUnittest::TestInitSASLSSL() {
    Json::Value authConfig;
    authConfig["SecurityProtocol"] = "sasl_ssl";
    authConfig["SASL"]["Username"] = "testuser";
    authConfig["SASL"]["Password"] = "testpass";
    authConfig["SASL"]["Mechanism"] = "PLAIN";
    authConfig["TLS"]["CAFile"] = "/path/to/ca.pem";

    APSARA_TEST_TRUE(mAuthenticator->Init(authConfig));
    APSARA_TEST_TRUE(mAuthenticator->IsAuthenticationEnabled());
    APSARA_TEST_TRUE(mAuthenticator->HasSaslConfig());
    APSARA_TEST_TRUE(mAuthenticator->HasTlsConfig());
}

void KafkaAuthenticatorUnittest::TestInitSASLWithKerberosKeytab() {
    Json::Value authConfig;
    authConfig["SecurityProtocol"] = "sasl_ssl";
    authConfig["SASL"]["Mechanism"] = "GSSAPI";
    authConfig["SASL"]["Kerberos"]["ServiceName"] = "kafka";
    authConfig["SASL"]["Kerberos"]["Principal"] = "user@EXAMPLE.COM";
    authConfig["SASL"]["Kerberos"]["UseKeyTab"] = true;
    authConfig["SASL"]["Kerberos"]["KeyTabPath"] = "/path/to/kafka.keytab";
    authConfig["TLS"]["CAFile"] = "/path/to/ca.pem";

    APSARA_TEST_TRUE(mAuthenticator->Init(authConfig));
    APSARA_TEST_TRUE(mAuthenticator->IsAuthenticationEnabled());
    APSARA_TEST_TRUE(mAuthenticator->HasSaslConfig());
    APSARA_TEST_TRUE(mAuthenticator->HasTlsConfig());
}

void KafkaAuthenticatorUnittest::TestInitSASLWithKerberosPassword() {
    Json::Value authConfig;
    authConfig["SecurityProtocol"] = "sasl_plaintext";
    authConfig["SASL"]["Mechanism"] = "GSSAPI";
    authConfig["SASL"]["Kerberos"]["ServiceName"] = "kafka";
    authConfig["SASL"]["Kerberos"]["Principal"] = "user@EXAMPLE.COM";
    authConfig["SASL"]["Kerberos"]["UseKeyTab"] = false;
    authConfig["SASL"]["Kerberos"]["Username"] = "user@EXAMPLE.COM";
    authConfig["SASL"]["Kerberos"]["Password"] = "krb-password";

    APSARA_TEST_TRUE(mAuthenticator->Init(authConfig));
    APSARA_TEST_TRUE(mAuthenticator->IsAuthenticationEnabled());
    APSARA_TEST_TRUE(mAuthenticator->HasSaslConfig());
    APSARA_TEST_FALSE(mAuthenticator->HasTlsConfig());
}

void KafkaAuthenticatorUnittest::TestConfigValidation() {
    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "plaintext";
        authConfig["SASL"]["Username"] = "testuser";
        authConfig["SASL"]["Password"] = "testpass";
        authConfig["SASL"]["Mechanism"] = "PLAIN";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_ssl";
        authConfig["SASL"]["Username"] = "testuser";
        authConfig["SASL"]["Password"] = "testpass";
        authConfig["SASL"]["Mechanism"] = "PLAIN";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "ssl";
        authConfig["SASL"]["Username"] = "testuser";
        authConfig["SASL"]["Password"] = "testpass";
        authConfig["SASL"]["Mechanism"] = "PLAIN";
        authConfig["TLS"]["CAFile"] = "/path/to/ca.pem";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_plaintext";
        authConfig["SASL"]["Username"] = "testuser";
        authConfig["SASL"]["Password"] = "testpass";
        authConfig["SASL"]["Mechanism"] = "PLAIN";
        authConfig["TLS"]["CAFile"] = "/path/to/ca.pem";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }
}

void KafkaAuthenticatorUnittest::TestInvalidConfig() {
    {
        Json::Value authConfig;
        authConfig["SASL"]["Username"] = "testuser";
        authConfig["SASL"]["Password"] = "testpass";
        authConfig["SASL"]["Mechanism"] = "PLAIN";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "invalid";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "plaintext";
        authConfig["OtherAuth"]["SomeField"] = "value";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_plaintext";
        authConfig["SASL"]["Username"] = "testuser";
        authConfig["SASL"]["Password"] = "testpass";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_plaintext";
        authConfig["SASL"]["Username"] = "testuser";
        authConfig["SASL"]["Password"] = "testpass";
        authConfig["SASL"]["Mechanism"] = "INVALID_MECH";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_plaintext";
        authConfig["SASL"]["Mechanism"] = "GSSAPI";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_plaintext";
        authConfig["SASL"]["Mechanism"] = "GSSAPI";
        authConfig["SASL"]["Kerberos"]["ServiceName"] = "kafka";
        authConfig["SASL"]["Kerberos"]["Principal"] = "user@EXAMPLE.COM";
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_plaintext";
        authConfig["SASL"]["Mechanism"] = "GSSAPI";
        authConfig["SASL"]["Kerberos"]["ServiceName"] = "kafka";
        authConfig["SASL"]["Kerberos"]["Principal"] = "user@EXAMPLE.COM";
        authConfig["SASL"]["Kerberos"]["UseKeyTab"] = true;
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }

    {
        Json::Value authConfig;
        authConfig["SecurityProtocol"] = "sasl_plaintext";
        authConfig["SASL"]["Mechanism"] = "GSSAPI";
        authConfig["SASL"]["Kerberos"]["ServiceName"] = "kafka";
        authConfig["SASL"]["Kerberos"]["Principal"] = "user@EXAMPLE.COM";
        authConfig["SASL"]["Kerberos"]["UseKeyTab"] = false;
        APSARA_TEST_FALSE(mAuthenticator->Init(authConfig));
    }
}

UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInitNone)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInitPlaintext)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInitSSL)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInitSASLPlaintext)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInitSASLSSL)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInitSASLWithKerberosKeytab)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInitSASLWithKerberosPassword)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestConfigValidation)
UNIT_TEST_CASE(KafkaAuthenticatorUnittest, TestInvalidConfig)

} // namespace logtail
UNIT_TEST_MAIN
