// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package custom

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/ava-labs/avalanchego/ids"
)

// MockCustomVMServer creates a mock HTTP server that simulates a custom VM API
// This is useful for testing the custom VM integration
type MockCustomVMServer struct {
	Server       *httptest.Server
	blocks       map[uint64]*CustomBlockInfo
	latestHeight uint64
	txCounter    uint64
}

// NewMockCustomVMServer creates a new mock custom VM server
func NewMockCustomVMServer() *MockCustomVMServer {
	mock := &MockCustomVMServer{
		blocks:       make(map[uint64]*CustomBlockInfo),
		latestHeight: 1000, // Start at block 1000
		txCounter:    1,
	}

	mux := http.NewServeMux()

	// JSON-RPC endpoint for all methods
	mux.HandleFunc("/", mock.handleJSONRPC)

	mock.Server = httptest.NewServer(mux)

	// Initialize with some test blocks
	mock.initializeTestBlocks()

	return mock
}

// Close shuts down the mock server
func (m *MockCustomVMServer) Close() {
	m.Server.Close()
}

// GetURL returns the mock server URL
func (m *MockCustomVMServer) GetURL() string {
	return m.Server.URL
}

// AddBlock adds a new block with warp messages to the mock server
func (m *MockCustomVMServer) AddBlock(messages []*CustomWarpMessage) uint64 {
	m.latestHeight++
	blockID := ids.GenerateTestID()

	m.blocks[m.latestHeight] = &CustomBlockInfo{
		BlockID:  blockID,
		Height:   m.latestHeight,
		Messages: messages,
	}

	return m.latestHeight
}

// AddWarpMessage adds a warp message to the specified block
func (m *MockCustomVMServer) AddWarpMessage(height uint64, sourceChainID, sourceAddr, unsignedMsgBytes string) {
	if block, exists := m.blocks[height]; exists {
		txID := fmt.Sprintf("%064x", m.txCounter)
		m.txCounter++

		message := &CustomWarpMessage{
			TxID:                 txID,
			SourceChainID:        sourceChainID,
			SourceAddress:        sourceAddr,
			Payload:              "", // Empty for now, can be set if needed
			UnsignedMessageBytes: unsignedMsgBytes,
		}

		block.Messages = append(block.Messages, message)
	}
}

// handleJSONRPC handles all JSON-RPC requests
func (m *MockCustomVMServer) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		JSONRPC string                 `json:"jsonrpc"`
		Method  string                 `json:"method"`
		Params  map[string]interface{} `json:"params"`
		ID      int                    `json:"id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	switch request.Method {
	case "warpcustomvm.getLatestBlock":
		m.handleGetLatestBlock(w, request.ID)
	case "warpcustomvm.getBlock":
		m.handleGetBlock(w, request.Params, request.ID)
	case "xsvm.issueTx":
		m.handleIssueTx(w, request.Params, request.ID)
	default:
		// Return JSON-RPC error for unknown method
		response := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      request.ID,
			"error": map[string]interface{}{
				"code":    -32601,
				"message": fmt.Sprintf("Method not found: %s", request.Method),
			},
		}
		json.NewEncoder(w).Encode(response)
	}
}

// handleGetLatestBlock handles warpcustomvm.getLatestBlock requests
func (m *MockCustomVMServer) handleGetLatestBlock(w http.ResponseWriter, id int) {
	response := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"result": map[string]interface{}{
			"height": m.latestHeight,
		},
	}
	json.NewEncoder(w).Encode(response)
}

// handleGetBlock handles warpcustomvm.getBlock requests
func (m *MockCustomVMServer) handleGetBlock(w http.ResponseWriter, params map[string]interface{}, id int) {
	heightFloat, ok := params["height"].(float64)
	if !ok {
		response := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      id,
			"error": map[string]interface{}{
				"code":    -32602,
				"message": "Invalid params: height required",
			},
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	height := uint64(heightFloat)

	// Get block info
	block, exists := m.blocks[height]
	if !exists {
		// Return empty block if it doesn't exist
		block = &CustomBlockInfo{
			BlockID:  ids.GenerateTestID(),
			Height:   height,
			Messages: []*CustomWarpMessage{},
		}
	}

	response := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"result": map[string]interface{}{
			"blockID":   block.BlockID,
			"height":    block.Height,
			"parentID":  ids.Empty,
			"timestamp": 0,
			"messages":  block.Messages,
		},
	}
	json.NewEncoder(w).Encode(response)
}

// handleIssueTx handles xsvm.issueTx requests
func (m *MockCustomVMServer) handleIssueTx(w http.ResponseWriter, params map[string]interface{}, id int) {
	// Generate a mock transaction response
	m.txCounter++
	m.latestHeight++ // Simulate new block with this transaction

	txID := ids.GenerateTestID()

	response := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"result": map[string]interface{}{
			"txID": txID,
		},
	}
	json.NewEncoder(w).Encode(response)
}

// initializeTestBlocks creates some initial test blocks
func (m *MockCustomVMServer) initializeTestBlocks() {
	sourceChainID := ids.GenerateTestID()

	// Block 1001 with one warp message
	m.blocks[1001] = &CustomBlockInfo{
		BlockID: ids.GenerateTestID(),
		Height:  1001,
		Messages: []*CustomWarpMessage{
			{
				TxID:                 fmt.Sprintf("%064x", 1),
				SourceChainID:        sourceChainID.String(),
				SourceAddress:        "0x1111111111111111111111111111111111111111",
				Payload:              "",
				UnsignedMessageBytes: generateMockWarpMessage(sourceChainID),
			},
		},
	}

	// Block 1002 with no warp messages
	m.blocks[1002] = &CustomBlockInfo{
		BlockID:  ids.GenerateTestID(),
		Height:   1002,
		Messages: []*CustomWarpMessage{},
	}

	// Block 1003 with multiple warp messages
	m.blocks[1003] = &CustomBlockInfo{
		BlockID: ids.GenerateTestID(),
		Height:  1003,
		Messages: []*CustomWarpMessage{
			{
				TxID:                 fmt.Sprintf("%064x", 2),
				SourceChainID:        sourceChainID.String(),
				SourceAddress:        "0x2222222222222222222222222222222222222222",
				Payload:              "",
				UnsignedMessageBytes: generateMockWarpMessage(sourceChainID),
			},
			{
				TxID:                 fmt.Sprintf("%064x", 3),
				SourceChainID:        sourceChainID.String(),
				SourceAddress:        "0x3333333333333333333333333333333333333333",
				Payload:              "",
				UnsignedMessageBytes: generateMockWarpMessage(sourceChainID),
			},
		},
	}

	m.latestHeight = 1003
	m.txCounter = 3
}

// generateMockWarpMessage generates a mock base64-encoded warp message
func generateMockWarpMessage(sourceChainID ids.ID) string {
	// This is a simplified mock - in reality, you'd generate a proper Avalanche Warp message
	mockPayload := []byte("Hello from custom VM!")

	// Simple concatenation for testing - real implementation would use proper Warp message format
	message := append(sourceChainID[:], mockPayload...)
	return base64.StdEncoding.EncodeToString(message)
}
