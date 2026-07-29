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

use chrono::{DateTime, TimeDelta, Utc};
use protobuf::{well_known_types::timestamp::Timestamp, EnumOrUnknown, MessageField};

use crate::{
    communication::{SubscriptionStatus, UPayload},
    core::usubscription::{RESOURCE_ID_SUBSCRIBE, RESOURCE_ID_UNSUBSCRIBE},
    up_core_api::{
        uri::UUri as UUriProto,
        usubscription::{
            SubscribeRequest as SubscribeRequestProto, SubscribeResponse as SubscribeResponseProto,
            UnsubscribeRequest as UnsubscribeRequestProto,
            UnsubscribeResponse as UnsubscribeResponseProto,
        },
    },
    ProtobufMappable, UAttributes, UCode, UStatus, UUri,
};

fn protobuf_timestamp_as_chrono_datetime(
    ts: Option<&Timestamp>,
) -> Result<Option<DateTime<Utc>>, UStatus> {
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

        // nanos already validated to be in [0, 1_000_000_000), so this cast is safe
        DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
            .ok_or_else(err)
            .map(Some)
    } else {
        Ok(None)
    }
}

/// Crate-internal glue that maps a protobuf request message to its public,
/// protobuf-free counterpart.
///
/// Implementing this trait for a public request type is all that is needed to
/// make it decodable via [`extract_request`]: the associated [`Proto`](Self::Proto)
/// type selects which protobuf message to deserialize, and [`from_proto`](Self::from_proto)
/// performs the mapping.
trait FromProtoRequestPayload: Sized {
    /// The protobuf message this request is transmitted as on the wire.
    type Proto: ProtobufMappable + Default;

    /// Builds the public request from the decoded protobuf message together with
    /// the source (caller) URI taken from the message attributes.
    fn from_proto(proto: Self::Proto, source: UUri) -> Result<Self, UStatus>;
}

// These are the uSubscription message types for a server implementation to work with.
// They exist to hide any serialization-specifics - no protobuf-generated types (no `MessageField`, no `Timestamp`),
// should be visible to the users of up-urst so the wire format stays an implementation detail of this crate.

/// A request to subscribe to a topic.
#[derive(Clone, Debug, PartialEq)]
pub struct SubscribeRequest {
    /// The uEntity that wants to subscribe (taken from the message's source address).
    pub subscriber: UUri,
    /// The topic to subscribe to.
    pub topic: UUri,
    /// The point in time at which the subscription expires.
    pub expiration: Option<DateTime<Utc>>,
    /// The minimum duration between two events (before they should be forwarded by a UStreamer).
    pub sample_period: Option<TimeDelta>,
}

impl FromProtoRequestPayload for SubscribeRequest {
    type Proto = SubscribeRequestProto;

    fn from_proto(proto: Self::Proto, subscriber: UUri) -> Result<Self, UStatus> {
        Ok(SubscribeRequest {
            subscriber,
            topic: require_topic(proto.topic)?,
            expiration: protobuf_timestamp_as_chrono_datetime(proto.expiration.as_ref())?,
            sample_period: proto
                .sample_period
                .map(|sp| TimeDelta::milliseconds(sp as i64)),
        })
    }
}

/// A request to unsubscribe from a topic.
#[derive(Clone, Debug, PartialEq)]
pub struct UnsubscribeRequest {
    /// The uEntity that wants to unsubscribe (taken from the message's source address).
    pub subscriber: UUri,
    /// The topic to unsubscribe from.
    pub topic: UUri,
}

impl FromProtoRequestPayload for UnsubscribeRequest {
    type Proto = UnsubscribeRequestProto;

    fn from_proto(proto: Self::Proto, subscriber: UUri) -> Result<Self, UStatus> {
        Ok(UnsubscribeRequest {
            subscriber,
            topic: require_topic(proto.topic)?,
        })
    }
}

/// A decoded uSubscription request, tagged by the operation it belongs to.
///
/// Returned by [`extract_usubscription_request`] so that a server can `match` on
/// the operation and handle it with fully unpacked, native Rust data.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum USubscriptionRequest {
    /// A [`RESOURCE_ID_SUBSCRIBE`] request.
    Subscribe(SubscribeRequest),
    /// A [`RESOURCE_ID_UNSUBSCRIBE`] request.
    Unsubscribe(UnsubscribeRequest),
}

/// Decodes an incoming uSubscription request into its public representation.
///
/// This is the single entry point a server uses to turn a raw protobuf payload
/// plus its message attributes into fully unpacked, native Rust data.
///
/// # Errors
///
/// Returns an error if `resource_id` does not identify a supported operation, if
/// the payload is missing, or if it cannot be deserialized into the expected type.
pub fn extract_usubscription_request(
    resource_id: u16,
    message_attributes: &UAttributes,
    request_payload: Option<UPayload>,
) -> Result<USubscriptionRequest, UStatus> {
    match resource_id {
        RESOURCE_ID_SUBSCRIBE => {
            extract_request_proto::<SubscribeRequest>(request_payload, message_attributes)
                .map(USubscriptionRequest::Subscribe)
        }
        RESOURCE_ID_UNSUBSCRIBE => {
            extract_request_proto::<UnsubscribeRequest>(request_payload, message_attributes)
                .map(USubscriptionRequest::Unsubscribe)
        }
        _ => Err(UStatus::fail_with_code(
            UCode::Unimplemented,
            format!("unsupported uSubscription resource id: {resource_id:#06x}"),
        )),
    }
}

/// Deserializes a request payload and maps it to its public representation.
///
/// The concrete target type `R` drives which protobuf message is expected and how
/// it is mapped, so this single generic implementation serves every operation.
fn extract_request_proto<R>(
    payload: Option<UPayload>,
    message_attributes: &UAttributes,
) -> Result<R, UStatus>
where
    R: FromProtoRequestPayload,
{
    let payload = payload.ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "missing request payload")
    })?;
    let proto: R::Proto = payload.extract_protobuf().map_err(|e| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("payload is not a valid: {e}"),
        )
    })?;
    R::from_proto(proto, message_attributes.source().clone())
}

/// Extracts and validates the topic from a protobuf request, failing if it is
/// absent or not a well-formed URI.
fn require_topic(topic: MessageField<UUriProto>) -> Result<UUri, UStatus> {
    let topic = topic
        .into_option()
        .ok_or_else(|| UStatus::fail_with_code(UCode::InvalidArgument, "missing topic"))?;
    UUri::try_from(&topic).map_err(|e| {
        UStatus::fail_with_code(UCode::InvalidArgument, format!("invalid topic URI: {e}"))
    })
}

// The mirror image of the request types above: a server builds these native
// structs and lets the crate turn them into a protobuf payload.

/// The counterpart of [`FromRequestPayload`]: implementing this trait for a public
/// response type is all that is needed to make it serializable via [`pack_response`],
/// again keeping the generated `*Proto` types hidden behind the public API.
trait IntoProtoResponsePayload {
    /// The protobuf message this response is transmitted as on the wire.
    type Proto: ProtobufMappable;

    /// Converts the public response into its protobuf representation.
    fn into_proto(self) -> Self::Proto;
}

/// The response to a [`SubscribeRequest`].
#[derive(Clone, Debug, PartialEq)]
pub struct SubscribeResponse {
    /// The topic the subscription refers to.
    pub topic: UUri,
    /// The resulting status of the subscription.
    pub status: SubscriptionStatus,
}

/// The response to an [`UnsubscribeRequest`].
///
/// The uSubscription service returns an empty message for this operation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UnsubscribeResponse;

/// A uSubscription response, tagged by the operation it belongs to.
///
/// Passed to [`pack_usubscription_response`] to obtain the protobuf payload that
/// is sent back to the client.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum USubscriptionResponse {
    /// A response to a [`RESOURCE_ID_SUBSCRIBE`] request.
    Subscribe(SubscribeResponse),
    /// A response to a [`RESOURCE_ID_UNSUBSCRIBE`] request.
    Unsubscribe(UnsubscribeResponse),
}

impl From<SubscribeResponse> for USubscriptionResponse {
    fn from(value: SubscribeResponse) -> Self {
        USubscriptionResponse::Subscribe(value)
    }
}

impl From<UnsubscribeResponse> for USubscriptionResponse {
    fn from(value: UnsubscribeResponse) -> Self {
        USubscriptionResponse::Unsubscribe(value)
    }
}

impl IntoProtoResponsePayload for SubscribeResponse {
    type Proto = SubscribeResponseProto;

    fn into_proto(self) -> Self::Proto {
        SubscribeResponseProto {
            topic: MessageField::some(UUriProto::from(&self.topic)),
            status: EnumOrUnknown::new((&self.status).into()),
            ..Default::default()
        }
    }
}

impl IntoProtoResponsePayload for UnsubscribeResponse {
    type Proto = UnsubscribeResponseProto;

    fn into_proto(self) -> Self::Proto {
        UnsubscribeResponseProto::default()
    }
}

/// Serializes a public response into an RPC response payload.
///
/// As with [`extract_request`], the concrete type drives which protobuf message is
/// produced, so this single generic implementation serves every operation.
fn pack_response_proto<R>(response: R) -> Result<UPayload, UStatus>
where
    R: IntoProtoResponsePayload,
{
    UPayload::try_from_protobuf(response.into_proto()).map_err(|e| {
        UStatus::fail_with_code(
            UCode::Internal,
            format!("failed to serialize response payload: {e}"),
        )
    })
}

/// Serializes a uSubscription response into the payload of an RPC response message.
///
/// This is the mirror image of [`extract_usubscription_request`]: a server builds a
/// native [`USubscriptionResponse`] and this function turns it into the protobuf
/// payload to hand back to the client.
///
/// # Errors
///
/// Returns an error if the response cannot be serialized.
pub fn pack_usubscription_response(response: USubscriptionResponse) -> Result<UPayload, UStatus> {
    match response {
        USubscriptionResponse::Subscribe(response) => pack_response_proto(response),
        USubscriptionResponse::Unsubscribe(response) => pack_response_proto(response),
    }
}
