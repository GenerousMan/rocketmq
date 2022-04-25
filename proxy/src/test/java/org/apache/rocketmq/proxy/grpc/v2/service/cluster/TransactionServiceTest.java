package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.connector.transaction.TransactionId;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateCheckRequest;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionServiceTest extends BaseServiceTest {

    private TransactionService transactionService;
    @Mock
    private ChannelManager channelManager;

    @Override
    public void beforeEach() throws Throwable {
        transactionService = new TransactionService(this.connectorManager, this.channelManager);
    }

    @Test
    public void testCheckTransactionState() {
        GrpcClientChannel channel = mock(GrpcClientChannel.class);

        when(channelManager.getClientIdList(anyString())).thenReturn(Lists.newArrayList("clientId"));
        when(channelManager.getChannel(anyString(), any())).thenReturn(channel);
        ArgumentCaptor<Object> flushDataCaptor = ArgumentCaptor.forClass(Object.class);
        when(channel.writeAndFlush(flushDataCaptor.capture())).thenReturn(null);

        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            RemotingHelper.string2SocketAddress("127.0.0.1:8080"),
            "71F99B78B6E261357FA259CCA6456118", 1234, 5678);
        transactionService.checkTransactionState(new TransactionStateCheckRequest(
            "group",
            1L,
            2L,
            "msgId",
            transactionId,
            createMessageExt("msgId", "msgId")
        ));

        Object flushData = flushDataCaptor.getValue();
        assertTrue(flushData instanceof TelemetryCommand);
        TelemetryCommand response = (TelemetryCommand) flushData;
        assertEquals(transactionId.getProxyTransactionId(), response.getRecoverOrphanedTransactionCommand().getTransactionId());
    }

    @Test
    public void testEndTransaction() throws Exception {
        TransactionId transactionId = TransactionId.genByBrokerTransactionId(
            RemotingHelper.string2SocketAddress("127.0.0.1:8080"),
            "71F99B78B6E261357FA259CCA6456118", 1234, 5678);
        ArgumentCaptor<String> brokerAddrCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<EndTransactionRequestHeader> headerCaptor = ArgumentCaptor.forClass(EndTransactionRequestHeader.class);
        doNothing().when(producerClient)
            .endTransaction(any(), brokerAddrCaptor.capture(), headerCaptor.capture());

        EndTransactionResponse response = transactionService.endTransaction(Context.current(), EndTransactionRequest.newBuilder()
            .setTransactionId(transactionId.getProxyTransactionId())
            .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(transactionId.getBrokerTransactionId(), headerCaptor.getValue().getTransactionId());
        assertEquals("127.0.0.1:8080", brokerAddrCaptor.getValue());
    }
}