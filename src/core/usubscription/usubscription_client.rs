/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
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

use std::sync::Arc;

use async_trait::async_trait;
use protobuf::well_known_types::timestamp::Timestamp;

use crate::{
    communication::{CallOptions, RpcClient, SubscriptionStatus},
    core::usubscription::{
        usubscription_uri, SubscriptionInfo, USubscription, RESOURCE_ID_FETCH_SUBSCRIPTIONS,
        RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_RESET, RESOURCE_ID_SUBSCRIBE,
        RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS, RESOURCE_ID_UNSUBSCRIBE,
    },
    up_core_api::usubscription::{
        FetchSubscriptionsRequest, FetchSubscriptionsResponse, NotificationsResponse,
        ResetResponse, SubscribeRequest, SubscribeResponse, Subscription, UnsubscribeRequest,
        UnsubscribeResponse,
    },
    UCode, UStatus, UUri,
};

fn unix_epoch_millis_as_protobuf_timestamp(
    millis: Option<u64>,
) -> Result<Option<Timestamp>, UStatus> {
    if let Some(milliseconds) = millis {
        // this will always yield a valid Timestamp as the maximum value of u64 (2^64 - 1) divided by 1000
        // is less than the maximum number of milliseconds that can be represented in an i64 (2^63 - 1)
        let seconds = (milliseconds / 1000_u64) as i64;
        let nanos = (milliseconds % 1000)
            .checked_mul(1_000_000)
            .ok_or_else(|| {
                UStatus::fail_with_code(UCode::InvalidArgument, "timestamp out of range")
            })
            .and_then(|s| {
                i32::try_from(s).map_err(|_| {
                    UStatus::fail_with_code(UCode::InvalidArgument, "timestamp out of range")
                })
            })?;
        Ok(Some(Timestamp {
            seconds,
            nanos,
            ..Default::default()
        }))
    } else {
        Ok(None)
    }
}

pub(super) fn protobuf_timestamp_as_unix_epoch_milliseconds(
    ts: Option<&Timestamp>,
) -> Result<Option<u64>, UStatus> {
    if let Some(ts) = ts {
        let err = || {
            UStatus::fail_with_code(
                UCode::InvalidArgument,
                "invalid timestamp: seconds value out of range",
            )
        };
        if ts.nanos < 0 || ts.nanos >= 1_000_000_000 {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "invalid timestamp: nanos value out of range",
            ));
        }
        u64::try_from(ts.seconds)
            .ok()
            .and_then(|s| s.checked_mul(1000))
            .and_then(|ms| ms.checked_add(ts.nanos as u64 / 1_000_000))
            .ok_or_else(err)
            .map(Some)
    } else {
        Ok(None)
    }
}

impl TryFrom<&Subscription> for SubscriptionInfo {
    type Error = UStatus;

    fn try_from(subscription_proto: &Subscription) -> Result<Self, Self::Error> {
        let topic = subscription_proto
            .topic
            .as_ref()
            .ok_or(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "topic missing",
            ))
            .and_then(|t| {
                UUri::try_from(t)
                    .map_err(|_| UStatus::fail_with_code(UCode::InvalidArgument, "invalid topic"))
            })?;

        let subscriber = subscription_proto
            .subscriber
            .as_ref()
            .ok_or(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "subscriber missing",
            ))
            .and_then(|s| {
                UUri::try_from(s).map_err(|_| {
                    UStatus::fail_with_code(UCode::InvalidArgument, "invalid subscriber")
                })
            })?;

        let status = subscription_proto
            .status
            .enum_value()
            .map_err(|_| {
                UStatus::fail_with_code(UCode::InvalidArgument, "subscription status missing")
            })
            .map(|s| SubscriptionStatus::try_from(&s))??;

        Ok(SubscriptionInfo::new(
            topic,
            subscriber,
            status,
            protobuf_timestamp_as_unix_epoch_milliseconds(subscription_proto.expiration.as_ref())?,
            subscription_proto.sample_period,
        ))
    }
}

/// A [`USubscription`] client implementation for invoking operations of a local USubscription service.
///
/// The client requires an [`RpcClient`] for performing the remote procedure calls.
pub struct RpcClientUSubscription {
    rpc_client: Arc<dyn RpcClient>,
}

impl RpcClientUSubscription {
    /// Creates a new Notifier for a given transport.
    ///
    /// # Arguments
    ///
    /// * `rpc_client` - The client to use for performing the remote procedure calls on the USubscription service.
    pub fn new(rpc_client: Arc<dyn RpcClient>) -> Self {
        RpcClientUSubscription { rpc_client }
    }

    fn default_call_options() -> CallOptions {
        CallOptions::for_rpc_request(5_000, None, None, None)
    }
}

#[async_trait]
impl USubscription for RpcClientUSubscription {
    async fn subscribe(
        &self,
        topic: &UUri,
        expiration: Option<u64>, // millis since Unix Epoch
        min_sample_period: Option<u32>,
    ) -> Result<SubscriptionStatus, UStatus> {
        let subscription_request = SubscribeRequest {
            topic: Some(topic.into()).into(),
            expiration: unix_epoch_millis_as_protobuf_timestamp(expiration)?.into(),
            sample_period: min_sample_period,
            ..Default::default()
        };
        self.rpc_client
            .invoke_proto_method::<_, SubscribeResponse>(
                usubscription_uri(RESOURCE_ID_SUBSCRIBE),
                Self::default_call_options(),
                subscription_request,
            )
            .await
            .map_err(UStatus::from)
            .and_then(|response| {
                response
                    .status
                    .enum_value()
                    .map_err(|_| {
                        UStatus::fail_with_code(
                            UCode::InvalidArgument,
                            "uSubscription returned invalid response: no subscription status",
                        )
                    })
                    .map(|s| SubscriptionStatus::try_from(&s))?
            })
    }

    async fn unsubscribe(&self, topic: &UUri) -> Result<(), UStatus> {
        let unsubscribe_request = UnsubscribeRequest {
            topic: Some(topic.into()).into(),
            ..Default::default()
        };
        self.rpc_client
            .invoke_proto_method::<_, UnsubscribeResponse>(
                usubscription_uri(RESOURCE_ID_UNSUBSCRIBE),
                Self::default_call_options(),
                unsubscribe_request,
            )
            .await
            .map(|_response| ())
            .map_err(UStatus::from)
    }

    async fn fetch_subscriptions(
        &self,
        topic_filter: Option<UUri>,
        subscriber_filter: Option<UUri>,
    ) -> Result<Vec<SubscriptionInfo>, UStatus> {
        let fetch_subscriptions_request = FetchSubscriptionsRequest {
            topic_filter: topic_filter
                .map(|u| crate::up_core_api::uri::UUri::from(&u))
                .into(),
            subscriber_filter: subscriber_filter
                .map(|u| crate::up_core_api::uri::UUri::from(&u))
                .into(),
            ..Default::default()
        };

        let response = self
            .rpc_client
            .invoke_proto_method::<_, FetchSubscriptionsResponse>(
                usubscription_uri(RESOURCE_ID_FETCH_SUBSCRIPTIONS),
                Self::default_call_options(),
                fetch_subscriptions_request,
            )
            .await?;

        Ok(response
            .subscriptions
            .iter()
            .map(SubscriptionInfo::try_from)
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn register_for_notifications(&self) -> Result<(), UStatus> {
        self.rpc_client
            .invoke_proto_method::<_, NotificationsResponse>(
                usubscription_uri(RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS),
                Self::default_call_options(),
                crate::up_core_api::usubscription::NotificationsRequest::default(),
            )
            .await
            .map(|_response| ())
            .map_err(UStatus::from)
    }

    async fn unregister_for_notifications(&self) -> Result<(), UStatus> {
        self.rpc_client
            .invoke_proto_method::<_, NotificationsResponse>(
                usubscription_uri(RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS),
                Self::default_call_options(),
                crate::up_core_api::usubscription::NotificationsRequest::default(),
            )
            .await
            .map(|_response| ())
            .map_err(UStatus::from)
    }

    async fn reset(&self) -> Result<(), UStatus> {
        self.rpc_client
            .invoke_proto_method::<_, ResetResponse>(
                usubscription_uri(RESOURCE_ID_RESET),
                Self::default_call_options(),
                crate::up_core_api::usubscription::ResetRequest::default(),
            )
            .await
            .map(|_response| ())
            .map_err(UStatus::from)
    }
}

#[cfg(test)]
mod tests {
    use mockall::Sequence;

    use super::*;
    use crate::{
        communication::{MockRpcClient, UPayload},
        up_core_api::usubscription::{
            FetchSubscriptionsRequest, NotificationsRequest, ResetRequest, SubscribeRequest,
        },
        UCode, UUri,
    };
    use std::sync::Arc;

    #[test]
    fn test_unix_epoch_millis_as_protobuf_timestamp() {
        assert!(
            unix_epoch_millis_as_protobuf_timestamp(Some(1_000)).is_ok_and(|ts| {
                ts == Some(Timestamp {
                    seconds: 1,
                    nanos: 0,
                    ..Default::default()
                })
            })
        );

        assert!(
            unix_epoch_millis_as_protobuf_timestamp(Some(1_234)).is_ok_and(|ts| {
                ts == Some(Timestamp {
                    seconds: 1,
                    nanos: 234_000_000,
                    ..Default::default()
                })
            })
        );

        assert!(unix_epoch_millis_as_protobuf_timestamp(Some(u64::MAX)).is_ok());
        assert!(unix_epoch_millis_as_protobuf_timestamp(None).is_ok_and(|ts| ts.is_none()));
    }

    #[test_case::test_case(10, 234_000_000 => matches Ok(Some(10_234)); "succeeds for valid timestamp")]
    #[test_case::test_case(-10, 234_000_000 => matches Err(UStatus {..}); "fails for negative seconds")]
    #[test_case::test_case(10, -1 => matches Err(UStatus {..}); "fails for nanos exceeding lower bound")]
    #[test_case::test_case(10, 1_000_000_000 => matches Err(UStatus {..}); "fails for nanos exeeding upper bound")]
    fn test_protobuf_timestamp_as_unix_epoch_milliseconds(
        seconds: i64,
        nanos: i32,
    ) -> Result<Option<u64>, UStatus> {
        let timestamp = Timestamp {
            seconds,
            nanos,
            ..Default::default()
        };
        protobuf_timestamp_as_unix_epoch_milliseconds(Some(&timestamp))
    }

    #[tokio::test]
    async fn test_subscribe_invokes_rpc_client() {
        let topic = UUri::try_from_parts("other", 0xd5a3, 0x01, 0xd3fe).unwrap();
        let expected_request = SubscribeRequest {
            topic: Some((&topic).into()).into(),
            ..Default::default()
        };
        let mut rpc_client = MockRpcClient::new();
        let mut seq = Sequence::new();
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(|method, _options, payload| {
                method == &usubscription_uri(RESOURCE_ID_SUBSCRIBE) && payload.is_some()
            })
            .return_const(Err(crate::communication::ServiceInvocationError::Internal(
                "internal error".to_string(),
            )));
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(move |method, _options, payload| {
                let request = payload
                    .to_owned()
                    .unwrap()
                    .extract_protobuf::<SubscribeRequest>()
                    .unwrap();
                request == expected_request && method == &usubscription_uri(RESOURCE_ID_SUBSCRIBE)
            })
            .returning(move |_method, _options, _payload| {
                let response = SubscribeResponse {
                    status:
                        crate::up_core_api::usubscription::SubscriptionStatus::STATUS_SUBSCRIBED
                            .into(),
                    ..Default::default()
                };
                Ok(Some(UPayload::try_from_protobuf(response).unwrap()))
            });

        let usubscription_client = RpcClientUSubscription::new(Arc::new(rpc_client));

        assert!(usubscription_client
            .subscribe(&topic, None, None)
            .await
            .is_err_and(|e| e.get_code() == UCode::Internal));
        assert!(usubscription_client
            .subscribe(&topic, None, None)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_unsubscribe_invokes_rpc_client() {
        let topic = UUri::try_from_parts("other", 0xd5a3, 0x01, 0xd3fe).unwrap();
        let expected_request = UnsubscribeRequest {
            topic: Some((&topic).into()).into(),
            ..Default::default()
        };
        let mut rpc_client = MockRpcClient::new();
        let mut seq = Sequence::new();
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(|method, _options, payload| {
                method == &usubscription_uri(RESOURCE_ID_UNSUBSCRIBE) && payload.is_some()
            })
            .return_const(Err(crate::communication::ServiceInvocationError::Internal(
                "internal error".to_string(),
            )));
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(move |method, _options, payload| {
                let request = payload
                    .to_owned()
                    .unwrap()
                    .extract_protobuf::<UnsubscribeRequest>()
                    .unwrap();
                request == expected_request && method == &usubscription_uri(RESOURCE_ID_UNSUBSCRIBE)
            })
            .returning(move |_method, _options, _payload| {
                let response = UnsubscribeResponse {
                    ..Default::default()
                };
                Ok(Some(UPayload::try_from_protobuf(response).unwrap()))
            });

        let usubscription_client = RpcClientUSubscription::new(Arc::new(rpc_client));

        assert!(usubscription_client
            .unsubscribe(&topic)
            .await
            .is_err_and(|e| e.get_code() == UCode::Internal));
        assert!(usubscription_client.unsubscribe(&topic).await.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_subscriptions_invokes_rpc_client() {
        let topic = UUri::try_from_parts("other", 0xd5a3, 0x01, 0xd3fe).unwrap();
        let expected_request = FetchSubscriptionsRequest {
            topic_filter: Some(crate::up_core_api::uri::UUri::from(&topic)).into(),
            ..Default::default()
        };
        let mut rpc_client = MockRpcClient::new();
        let mut seq = Sequence::new();
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(|method, _options, payload| {
                method == &usubscription_uri(RESOURCE_ID_FETCH_SUBSCRIPTIONS) && payload.is_some()
            })
            .return_const(Err(crate::communication::ServiceInvocationError::Internal(
                "internal error".to_string(),
            )));
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(move |method, _options, payload| {
                let request = payload
                    .to_owned()
                    .unwrap()
                    .extract_protobuf::<FetchSubscriptionsRequest>()
                    .unwrap();

                request == expected_request
                    && method == &usubscription_uri(RESOURCE_ID_FETCH_SUBSCRIPTIONS)
            })
            .returning(move |_method, _options, _payload| {
                let response = FetchSubscriptionsResponse {
                    ..Default::default()
                };
                Ok(Some(UPayload::try_from_protobuf(response).unwrap()))
            });

        let usubscription_client = RpcClientUSubscription::new(Arc::new(rpc_client));

        assert!(usubscription_client
            .fetch_subscriptions(Some(topic.clone()), None)
            .await
            .is_err_and(|e| e.get_code() == UCode::Internal));
        assert!(usubscription_client
            .fetch_subscriptions(Some(topic), None)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_register_for_notifications_invokes_rpc_client() {
        let mut rpc_client = MockRpcClient::new();
        let mut seq = Sequence::new();
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(|method, _options, payload| {
                method == &usubscription_uri(RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS)
                    && payload.is_some()
            })
            .return_const(Err(crate::communication::ServiceInvocationError::Internal(
                "internal error".to_string(),
            )));
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(move |method, _options, payload| {
                let request = payload
                    .to_owned()
                    .unwrap()
                    .extract_protobuf::<NotificationsRequest>()
                    .unwrap();

                request == NotificationsRequest::default()
                    && method == &usubscription_uri(RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS)
            })
            .returning(move |_method, _options, _payload| {
                let response = NotificationsResponse {
                    ..Default::default()
                };
                Ok(Some(UPayload::try_from_protobuf(response).unwrap()))
            });

        let usubscription_client = RpcClientUSubscription::new(Arc::new(rpc_client));

        assert!(usubscription_client
            .register_for_notifications()
            .await
            .is_err_and(|e| e.get_code() == UCode::Internal));
        assert!(usubscription_client
            .register_for_notifications()
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_unregister_for_notifications_invokes_rpc_client() {
        let mut rpc_client = MockRpcClient::new();
        let mut seq = Sequence::new();
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(|method, _options, payload| {
                method == &usubscription_uri(RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS)
                    && payload.is_some()
            })
            .return_const(Err(crate::communication::ServiceInvocationError::Internal(
                "internal error".to_string(),
            )));
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(move |method, _options, payload| {
                let request = payload
                    .to_owned()
                    .unwrap()
                    .extract_protobuf::<NotificationsRequest>()
                    .unwrap();

                request == NotificationsRequest::default()
                    && method == &usubscription_uri(RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS)
            })
            .returning(move |_method, _options, _payload| {
                let response = NotificationsResponse {
                    ..Default::default()
                };
                Ok(Some(UPayload::try_from_protobuf(response).unwrap()))
            });

        let usubscription_client = RpcClientUSubscription::new(Arc::new(rpc_client));

        assert!(usubscription_client
            .unregister_for_notifications()
            .await
            .is_err_and(|e| e.get_code() == UCode::Internal));
        assert!(usubscription_client
            .unregister_for_notifications()
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_reset_invokes_rpc_client() {
        let mut rpc_client = MockRpcClient::new();
        let mut seq = Sequence::new();
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(|method, _options, payload| {
                method == &usubscription_uri(RESOURCE_ID_RESET) && payload.is_some()
            })
            .return_const(Err(crate::communication::ServiceInvocationError::Internal(
                "internal error".to_string(),
            )));
        rpc_client
            .expect_invoke_method()
            .once()
            .in_sequence(&mut seq)
            .withf(move |method, _options, payload| {
                let request = payload
                    .to_owned()
                    .unwrap()
                    .extract_protobuf::<ResetRequest>()
                    .unwrap();

                request == ResetRequest::default()
                    && method == &usubscription_uri(RESOURCE_ID_RESET)
            })
            .returning(move |_method, _options, _payload| {
                let response = ResetResponse {
                    ..Default::default()
                };
                Ok(Some(UPayload::try_from_protobuf(response).unwrap()))
            });

        let usubscription_client = RpcClientUSubscription::new(Arc::new(rpc_client));

        assert!(usubscription_client
            .reset()
            .await
            .is_err_and(|e| e.get_code() == UCode::Internal));
        assert!(usubscription_client.reset().await.is_ok());
    }
}
