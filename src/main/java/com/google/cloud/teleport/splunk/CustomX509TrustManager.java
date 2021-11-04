/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.splunk;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * A Custom X509TrustManager that trusts a user provided self signed certificate and default CA's.
 */
public class CustomX509TrustManager implements X509TrustManager {

    X509TrustManager defaultTrustManager;
    X509Certificate userCertificate;

    public CustomX509TrustManager(X509Certificate userCertificate)
        throws KeyStoreException, NoSuchAlgorithmException {
        this.userCertificate = userCertificate;
        //Get Default Trust Manager
        TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm());
        trustMgrFactory.init((KeyStore) null);
        for (TrustManager tm : trustMgrFactory.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                defaultTrustManager = (X509TrustManager) tm;
                break;
            }
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
        defaultTrustManager.checkClientTrusted(chain, authType);
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        try {
            defaultTrustManager.checkServerTrusted(chain, authType);
        } catch (CertificateException ce) {
            /* Handle untrusted certificates */
            if (chain[0] != userCertificate) {
                throw ce;
            }
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return defaultTrustManager.getAcceptedIssuers();
    }
}
