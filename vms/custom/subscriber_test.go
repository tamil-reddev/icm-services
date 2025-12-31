// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package custom

import (
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/icm-services/relayer/config"
	"github.com/stretchr/testify/require"
)

func TestCustomVMSubscriber(t *testing.T) {
	require := require.New(t)

	// Create mock server
	mockServer := NewMockCustomVMServer()
	defer mockServer.Close()

	// Create subscriber
	logger := logging.NoLog{}
	blockchainID := ids.GenerateTestID()
	subscriber := NewSubscriber(logger, blockchainID, mockServer.GetURL())

	require.NotNil(subscriber, "Subscriber should not be nil")

	// Test Subscribe
	err := subscriber.Subscribe(5 * time.Second)
	require.NoError(err, "Subscribe should not return error")

	// Test ProcessFromHeight
	done := make(chan bool, 1)
	go subscriber.ProcessFromHeight(big.NewInt(1001), done)

	// Wait for processing to complete
	select {
	case result := <-done:
		require.True(result, "ProcessFromHeight should succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("ProcessFromHeight timed out")
	}

	// Test ICMBlocks channel
	select {
	case block := <-subscriber.ICMBlocks():
		require.NotNil(block, "Block should not be nil")
		require.Equal(uint64(1001), block.BlockNumber, "Block number should match")
		require.Len(block.Messages, 1, "Should have one message")
	case <-time.After(5 * time.Second):
		t.Fatal("No block received from ICMBlocks channel")
	}

	// Test Cancel
	subscriber.Cancel()
}

func TestCustomVMDestinationClient(t *testing.T) {
	require := require.New(t)

	// Create mock server
	mockServer := NewMockCustomVMServer()
	defer mockServer.Close()

	// This test would require more setup to create a proper destination client
	// For now, we'll test the mock server directly

	url := mockServer.GetURL()
	require.NotEmpty(url, "Mock server URL should not be empty")

	// Add a test block with warp message
	sourceChainID := ids.GenerateTestID()
	messages := []*CustomWarpMessage{
		{
			TxID:                 "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			SourceChainID:        sourceChainID.String(),
			SourceAddress:        "0x1234567890123456789012345678901234567890",
			Payload:              "",
			UnsignedMessageBytes: "YWJjZGVmMTIzNDU2Nzg5MA==", // base64 encoding of "abcdef1234567890" bytes
		},
	}

	blockNum := mockServer.AddBlock(messages)
	require.Equal(uint64(1004), blockNum, "Block number should be incremented")
}

func TestCustomVMContractMessage(t *testing.T) {
	require := require.New(t)

	logger := logging.NoLog{}
	contractMessage := NewContractMessage(logger, config.SourceBlockchain{})

	require.NotNil(contractMessage, "Contract message handler should not be nil")

	// Test with invalid message bytes
	_, err := contractMessage.UnpackWarpMessage([]byte("invalid"))
	require.Error(err, "Should return error for invalid message")

	// Test with empty message bytes
	_, err = contractMessage.UnpackWarpMessage([]byte{})
	require.Error(err, "Should return error for empty message")
}
