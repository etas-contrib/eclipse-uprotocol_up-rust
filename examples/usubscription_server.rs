/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

/*!
This example illustrates how a service provider implements the server side of the
uSubscription service.

A [`RpcClientUSubscription`] drives the example by issuing `subscribe`/`unsubscribe`
calls over an in-process [`LocalTransport`].
 */

use std::{collections::HashSet, sync::Arc};

use tokio::sync::Mutex;
use up_rust::{
    communication::{
        InMemoryRpcClient, InMemoryRpcServer, RequestHandler, RpcServer, ServiceInvocationError,
        SubscriptionStatus, UPayload,
    },
    core::usubscription::{
        extract_usubscription_request, pack_usubscription_response, RpcClientUSubscription,
        SubscribeResponse, USubscription, USubscriptionRequest, USubscriptionResponse,
        UnsubscribeResponse, RESOURCE_ID_SUBSCRIBE, RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID,
        USUBSCRIPTION_VERSION_MAJOR,
    },
    local_transport::LocalTransport,
    StaticUriProvider, UAttributes, UUri,
};

/// A minimal, in-memory implementation of the uSubscription service.
///
/// It keeps track of the `(subscriber, topic)` pairs it has seen so far. A real
/// implementation would persist this state and enforce the uSubscription state machine.
#[derive(Default)]
struct MyUSubscriptionService {
    subscriptions: Mutex<HashSet<(String, String)>>,
}

#[async_trait::async_trait]
impl RequestHandler for MyUSubscriptionService {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // Decode the payload of *any* uSubscription operation into a typed request. Malformed or
        // unsupported requests result in a `ServiceInvocationError`.
        let request =
            extract_usubscription_request(resource_id, message_attributes, request_payload)?;

        let response: USubscriptionResponse = match request {
            USubscriptionRequest::Subscribe(req) => {
                println!(
                    "SUBSCRIBE   subscriber={}, topic={}, expiration={:?}, sample_period={:?}",
                    req.subscriber, req.topic, req.expiration, req.sample_period
                );
                self.subscriptions
                    .lock()
                    .await
                    .insert((req.subscriber.to_string(), req.topic.to_string()));
                SubscribeResponse {
                    topic: req.topic,
                    status: SubscriptionStatus::Subscribed,
                }
                .into()
            }
            USubscriptionRequest::Unsubscribe(req) => {
                println!(
                    "UNSUBSCRIBE subscriber={}, topic={}",
                    req.subscriber, req.topic
                );
                self.subscriptions
                    .lock()
                    .await
                    .remove(&(req.subscriber.to_string(), req.topic.to_string()));
                UnsubscribeResponse.into()
            }
            // `USubscriptionRequest` is `#[non_exhaustive]`, so new operations can be
            // added in future releases without breaking this code.
            other => {
                return Err(ServiceInvocationError::Unimplemented(format!(
                    "operation not supported by this service: {other:?}"
                )))
            }
        };

        // Serialize the native response into the RPC response payload.
        let payload = pack_usubscription_response(response)?;
        Ok(Some(payload))
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Using the LocalTransport lets us run the service and its client in the same
    // process. A real deployment would use a networked transport (e.g. MQTT5 or Zenoh).
    let transport = Arc::new(LocalTransport::default());

    // The uSubscription service lives at a well-known address: empty (local) authority,
    // the reserved entity ID and the service's major version.
    let service_uri_provider = Arc::new(StaticUriProvider::new(
        "",
        USUBSCRIPTION_TYPE_ID,
        USUBSCRIPTION_VERSION_MAJOR,
    )?);

    // Stand up the server and register our service for the subscribe/unsubscribe methods.
    let rpc_server = InMemoryRpcServer::new(transport.clone(), service_uri_provider);
    let service = Arc::new(MyUSubscriptionService::default());
    rpc_server
        .register_endpoint(None, RESOURCE_ID_SUBSCRIBE, service.clone())
        .await?;
    rpc_server
        .register_endpoint(None, RESOURCE_ID_UNSUBSCRIBE, service.clone())
        .await?;

    // Now act as a client that subscribes to and unsubscribes from a topic. The
    // subscriber's identity (its source address) is taken from this URI provider and
    // ends up in `SubscribeRequest::subscriber` on the server side.
    let client_uri_provider = Arc::new(StaticUriProvider::new("my-vehicle", 0xABCD, 0x01)?);
    let rpc_client = Arc::new(InMemoryRpcClient::new(transport, client_uri_provider).await?);
    let usubscription = RpcClientUSubscription::new(rpc_client);

    let topic = UUri::try_from_parts("my-vehicle", 0x0000_800A, 0x01, 0x8001)?;

    let status = usubscription.subscribe(&topic, None, None).await?;
    println!("client: subscribe call returned status {status:?}");

    usubscription.unsubscribe(&topic).await?;
    println!("client: unsubscribe call completed");

    Ok(())
}
