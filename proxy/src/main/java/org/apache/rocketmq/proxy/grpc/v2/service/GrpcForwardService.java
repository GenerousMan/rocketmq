/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.StartAndShutdown;

public interface GrpcForwardService extends StartAndShutdown {

    CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request);

    CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request);

    CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request);

    CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request);

    CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request);

    CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request);

    CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request);

    CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx, ForwardMessageToDeadLetterQueueRequest request);

    CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request);

    CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx, NotifyClientTerminationRequest request);

    CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx, ChangeInvisibleDurationRequest request);

    StreamObserver<TelemetryCommand> telemetry(Context ctx, StreamObserver<TelemetryCommand> responseObserver);
}
