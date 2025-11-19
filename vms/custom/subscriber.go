// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package custom

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	avalancheWarp "github.com/ava-labs/avalanchego/vms/platformvm/warp"
	warpPayload "github.com/ava-labs/avalanchego/vms/platformvm/warp/payload"
	relayerTypes "github.com/ava-labs/icm-services/types"
	"github.com/ava-labs/icm-services/utils"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
	"go.uber.org/zap"
)

const (
	// Max buffer size for custom VM subscription channels
	maxClientSubscriptionBuffer = 20000
	MaxBlocksPerRequest         = 200
	// Poll interval for custom VM block polling (since it doesn't support WebSocket subscriptions)
	defaultPollingInterval = 5 * time.Second

	// Error messages
	errFetchingLatestBlockHeight   = "Error fetching latest block height"
	errFetchingWarpMessageByHeight = "Error fetching warp messages by block height"
)

// Custom VM Warp message structure as expected from the VM API
type CustomWarpMessage struct {
	TxID                 string `json:"messageID"`
	SourceChainID        string `json:"sourceChainID"`
	SourceAddress        string `json:"sourceAddress"`
	Payload              string `json:"payload"`              // base64-encoded payload
	UnsignedMessageBytes string `json:"unsignedMessageBytes"` // hex-encoded unsigned warp message
}

// Custom VM block information
type CustomBlockInfo struct {
	BlockID  ids.ID               `json:"blockID"`
	Height   uint64               `json:"height"`
	Messages []*CustomWarpMessage `json:"messages"`
}

// Subscriber implements Subscriber interface for custom VMs
type Subscriber struct {
	rpcClient       *http.Client
	baseURL         string
	blockchainID    ids.ID
	icmBlocks       chan *relayerTypes.WarpBlockInfo
	errChan         chan error
	logger          logging.Logger
	pollingInterval time.Duration
	stopPolling     chan struct{}
	latestProcessed uint64
}

// NewSubscriber returns a custom VM subscriber
func NewSubscriber(
	logger logging.Logger,
	blockchainID ids.ID,
	rpcBaseURL string,
) *Subscriber {
	subscriber := &Subscriber{
		blockchainID:    blockchainID,
		rpcClient:       &http.Client{Timeout: utils.DefaultRPCTimeout},
		baseURL:         rpcBaseURL,
		logger:          logger,
		icmBlocks:       make(chan *relayerTypes.WarpBlockInfo, maxClientSubscriptionBuffer),
		errChan:         make(chan error),
		pollingInterval: defaultPollingInterval,
		stopPolling:     make(chan struct{}),
		latestProcessed: 0,
	}

	return subscriber
}

// ProcessFromHeight processes events from {height} to the latest block for custom VM
func (s *Subscriber) ProcessFromHeight(height *big.Int, done chan bool) {
	defer close(done)
	if height == nil {
		s.logger.Error("Cannot process logs from nil height")
		done <- false
		return
	}

	s.logger.Debug(
		"Processing historical blocks for custom VM",
		zap.Uint64("fromBlockHeight", height.Uint64()),
		zap.String("blockchainID", s.blockchainID.String()),
	)

	// Get the latest block height from custom VM
	latestBlockHeight, err := s.getLatestBlockHeight()
	if err != nil {
		s.logger.Error(
			"Failed to get latest block height for custom VM",
			zap.String("blockchainID", s.blockchainID.String()),
			zap.Error(err),
		)
		done <- false
		return
	}

	// Process blocks in batches
	for fromBlock := height.Uint64(); fromBlock <= latestBlockHeight; fromBlock += MaxBlocksPerRequest {
		toBlock := fromBlock + MaxBlocksPerRequest - 1
		if toBlock > latestBlockHeight {
			toBlock = latestBlockHeight
		}

		err = s.processBlockRange(fromBlock, toBlock)
		if err != nil {
			s.logger.Error("Failed to process block range for custom VM", zap.Error(err))
			done <- false
			return
		}
	}

	s.latestProcessed = latestBlockHeight
	done <- true
}

// Subscribe starts polling for new blocks in custom VM (no WebSocket support)
func (s *Subscriber) Subscribe(retryTimeout time.Duration) error {
	s.logger.Debug(
		"Starting custom VM block polling subscription",
		zap.String("blockchainID", s.blockchainID.String()),
		zap.Duration("pollingInterval", s.pollingInterval),
	)

	go s.pollForNewBlocks()
	return nil
}

// ICMBlocks returns the channel that receives processed blocks
func (s *Subscriber) ICMBlocks() <-chan *relayerTypes.WarpBlockInfo {
	return s.icmBlocks
}

// SubscribeErr returns the error channel for subscription errors
func (s *Subscriber) SubscribeErr() <-chan error {
	// Custom VM doesn't have WebSocket subscriptions, so this returns a dummy channel
	return make(<-chan error)
}

// Err returns the error channel for processing errors
func (s *Subscriber) Err() <-chan error {
	return s.errChan
}

// Cancel stops the polling subscription
func (s *Subscriber) Cancel() {
	close(s.stopPolling)
}

// GetLatestBlockHeight returns the latest block height from the custom VM
// This is a public wrapper around the private getLatestBlockHeight method
func (s *Subscriber) GetLatestBlockHeight() (uint64, error) {
	return s.getLatestBlockHeight()
}

// pollForNewBlocks continuously polls for new blocks
func (s *Subscriber) pollForNewBlocks() {
	ticker := time.NewTicker(s.pollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			latestHeight, err := s.getLatestBlockHeight()
			if err != nil {
				s.logger.Warn("Failed to get latest block height, will retry", zap.Error(err))
				// Don't exit on error, just continue to next tick
				// This allows the subscriber to recover when the VM comes back online
				continue
			}

			// Process any new blocks since the last processed block
			if latestHeight > s.latestProcessed {
				err = s.processBlockRange(s.latestProcessed+1, latestHeight)
				if err != nil {
					s.logger.Warn("Failed to process new blocks, will retry", zap.Error(err))
					// Don't exit on error, just continue to next tick
					// This allows recovery from transient errors
					continue
				}
				s.latestProcessed = latestHeight
			}

		case <-s.stopPolling:
			s.logger.Debug("Stopping custom VM polling subscription")
			return
		}
	}
}

// processBlockRange processes blocks in the given range for custom VM
func (s *Subscriber) processBlockRange(fromBlock, toBlock uint64) error {
	s.logger.Debug(
		"Processing custom VM block range",
		zap.Uint64("fromBlockHeight", fromBlock),
		zap.Uint64("toBlockHeight", toBlock),
		zap.String("blockchainID", s.blockchainID.String()),
	)

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		blockInfo, err := s.getBlock(blockNum)
		if err != nil {
			s.logger.Error(
				"Failed to get warp messages for block",
				zap.Uint64("blockNumber", blockNum),
				zap.Error(err),
			)
			return err
		}

		s.logger.Debug(
			"Retrieved block info from custom VM",
			zap.Uint64("blockNumber", blockNum),
			zap.Int("messageCount", len(blockInfo.Messages)),
		)

		// Skip blocks with no messages
		if len(blockInfo.Messages) == 0 {
			s.logger.Debug(
				"No warp messages in block, skipping",
				zap.Uint64("blockNumber", blockNum),
			)
			continue
		}

		// Convert custom VM messages to standard WarpBlockInfo format
		warpBlockInfo, err := s.convertToWarpBlockInfo(blockInfo)
		if err != nil {
			s.logger.Error(
				"Failed to convert custom VM block info",
				zap.Uint64("blockNumber", blockNum),
				zap.Error(err),
			)
			return err
		}

		// Skip if no valid messages after conversion (e.g., all had empty unsigned messages)
		if warpBlockInfo == nil {
			s.logger.Debug(
				"No valid warp messages after conversion, skipping block",
				zap.Uint64("blockNumber", blockNum),
			)
			continue
		}

		s.logger.Debug(
			"Sending warp messages to relayer for processing",
			zap.Uint64("blockNumber", blockNum),
			zap.Int("messageCount", len(warpBlockInfo.Messages)),
		)

		s.icmBlocks <- warpBlockInfo

		s.logger.Debug(
			"Successfully queued warp messages for relaying",
			zap.Uint64("blockNumber", blockNum),
			zap.Int("messageCount", len(warpBlockInfo.Messages)),
		)
	}

	return nil
}

// getLatestBlockHeight gets the latest block height from custom VM using JSON-RPC
func (s *Subscriber) getLatestBlockHeight() (uint64, error) {
	s.logger.Debug(
		"Calling warpcustomvm.getLatestBlock JSON-RPC method",
		zap.String("baseURL", s.baseURL),
		zap.String("blockchainID", s.blockchainID.String()),
	)

	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "warpcustomvm.getLatestBlock",
		"params":  map[string]interface{}{},
		"id":      1,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		s.logger.Error(
			"Failed to marshal JSON-RPC request for warpcustomvm.getLatestBlock",
			zap.Error(err),
		)
		return 0, fmt.Errorf("failed to marshal JSON-RPC request: %w", err)
	}

	resp, err := s.rpcClient.Post(
		s.baseURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		s.logger.Error(
			"Failed to call warpcustomvm.getLatestBlock JSON-RPC",
			zap.String("baseURL", s.baseURL),
			zap.Error(err),
		)
		return 0, fmt.Errorf("failed to call JSON-RPC: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		s.logger.Error(
			"HTTP error when calling warpcustomvm.getLatestBlock",
			zap.Int("statusCode", resp.StatusCode),
			zap.String("baseURL", s.baseURL),
		)
		return 0, fmt.Errorf("HTTP error %d when calling JSON-RPC", resp.StatusCode)
	}

	var rpcResponse struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  struct {
			Height uint64 `json:"height"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"messages"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResponse); err != nil {
		s.logger.Error(
			"Failed to decode warpcustomvm.getLatestBlock JSON-RPC response",
			zap.Error(err),
		)
		return 0, fmt.Errorf("failed to decode JSON-RPC response: %w", err)
	}

	if rpcResponse.Error != nil {
		s.logger.Error(
			"warpcustomvm.getLatestBlock returned JSON-RPC error",
			zap.Int("errorCode", rpcResponse.Error.Code),
			zap.String("errorMessage", rpcResponse.Error.Message),
		)
		return 0, fmt.Errorf("JSON-RPC error %d: %s", rpcResponse.Error.Code, rpcResponse.Error.Message)
	}

	s.logger.Debug(
		"Successfully retrieved latest block height via JSON-RPC",
		zap.Uint64("height", rpcResponse.Result.Height),
		zap.String("blockchainID", s.blockchainID.String()),
	)

	return rpcResponse.Result.Height, nil
}

// getBlock gets Warp messages for a specific block from custom VM
func (s *Subscriber) getBlock(blockNum uint64) (*CustomBlockInfo, error) {
	// Try JSON-RPC first (for VMs like xsvm)
	blockInfo, err := s.getBlockWarpMessages(blockNum)
	if err == nil {
		return blockInfo, nil
	}

	s.logger.Debug(
		"JSON-RPC method failed for block messages, falling back to REST API",
		zap.Uint64("blockNumber", blockNum),
		zap.Error(err),
	)

	// Fallback to REST API
	return nil, fmt.Errorf(errFetchingWarpMessageByHeight)
}

// getBlockWarpMessages gets block warp messages using JSON-RPC (xsvm.blockWarpMessages)
func (s *Subscriber) getBlockWarpMessages(height uint64) (*CustomBlockInfo, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "warpcustomvm.getBlock",
		"params": map[string]interface{}{
			"height": height,
		},
		"id": 1,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON-RPC request: %w", err)
	}

	resp, err := s.rpcClient.Post(
		s.baseURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to call JSON-RPC: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error %d when calling JSON-RPC", resp.StatusCode)
	}

	var rpcResponse struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  struct {
			BlockID   ids.ID               `json:"blockID"`
			ParentID  ids.ID               `json:"parentID"`
			Height    uint64               `json:"height"`
			Timestamp int64                `json:"timestamp"`
			Messages  []*CustomWarpMessage `json:"messages"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResponse); err != nil {
		return nil, fmt.Errorf("failed to decode JSON-RPC response: %w", err)
	}

	if rpcResponse.Error != nil {
		return nil, fmt.Errorf("JSON-RPC error %d: %s", rpcResponse.Error.Code, rpcResponse.Error.Message)
	}

	return &CustomBlockInfo{
		Height:   rpcResponse.Result.Height,
		BlockID:  rpcResponse.Result.BlockID,
		Messages: rpcResponse.Result.Messages,
	}, nil
}

// convertToWarpBlockInfo converts custom VM block info to standard WarpBlockInfo
func (s *Subscriber) convertToWarpBlockInfo(customBlock *CustomBlockInfo) (*relayerTypes.WarpBlockInfo, error) {
	s.logger.Debug(
		"Converting custom VM block to WarpBlockInfo",
		zap.Int("messageCount", len(customBlock.Messages)),
	)

	s.logger.Debug(
		"Converting custom VM block to WarpBlockInfo Height",
		zap.Uint64("Height", customBlock.Height),
	)

	messages := make([]*relayerTypes.WarpMessageInfo, 0, len(customBlock.Messages))

	for i, customMsg := range customBlock.Messages {
		s.logger.Debug(
			"Processing warp message from custom VM",
			zap.Int("messageIndex", i),
			zap.String("txID", customMsg.TxID),
			zap.String("sourceChainID", customMsg.SourceChainID),
			zap.Int("unsignedMessageLength", len(customMsg.UnsignedMessageBytes)),
			zap.String("payload", customMsg.Payload),
			zap.String("addressStr", customMsg.SourceAddress),
		)

		// Skip messages with empty unsigned message content
		// This can happen if the Custom VM hasn't fully exported the message yet
		// or if the message format is not yet supported
		if customMsg.UnsignedMessageBytes == "" {
			s.logger.Warn(
				"Skipping warp message with empty unsigned message content",
				zap.Int("messageIndex", i),
				zap.String("txID", customMsg.TxID),
				zap.String("sourceChainID", customMsg.SourceChainID),
			)
			continue
		}

		// Parse the unsigned message from base64
		unsignedMsg, err := s.parseUnsignedMessage(customMsg.UnsignedMessageBytes)
		if err != nil {
			s.logger.Error(
				"Failed to parse unsigned warp message",
				zap.Int("messageIndex", i),
				zap.String("unsignedMessage", customMsg.UnsignedMessageBytes),
				zap.Error(err),
			)
			return nil, fmt.Errorf("failed to parse unsigned message: %w", err)
		}

		s.logger.Debug(
			"Successfully parsed unsigned warp message",
			zap.Int("messageIndex", i),
			zap.String("sourceChainID", unsignedMsg.SourceChainID.String()),
		)

		// For Teleporter messages (AddressedCall payload), extract the source contract address
		// from the payload. For other message types, use the provided source address.
		var sourceAddr [20]byte
		if customMsg.SourceAddress == "" {
			// If Custom VM doesn't provide sourceAddress, try to extract it from the warp payload
			sourceAddr, err = s.extractSourceAddressFromPayload(unsignedMsg)
			if err != nil {
				s.logger.Error(
					"Failed to extract source address from warp payload",
					zap.Int("messageIndex", i),
					zap.Error(err),
				)
				return nil, fmt.Errorf("failed to extract source address from payload: %w", err)
			}
			s.logger.Debug(
				"Extracted source address from warp message payload",
				zap.Int("messageIndex", i),
				zap.String("sourceAddress", fmt.Sprintf("0x%x", sourceAddr)),
			)
		} else {
			// Convert source address provided by Custom VM
			sourceAddr, err = s.parseSourceAddress(customMsg.SourceAddress)
			if err != nil {
				s.logger.Error(
					"Failed to parse source address",
					zap.Int("messageIndex", i),
					zap.String("sourceAddress", customMsg.SourceAddress),
					zap.Error(err),
				)
				return nil, fmt.Errorf("failed to parse source address: %w", err)
			}
		}

		// Convert transaction ID
		txID, err := s.parseTxID(customMsg.TxID)
		if err != nil {
			s.logger.Error(
				"Failed to parse transaction ID",
				zap.Int("messageIndex", i),
				zap.String("txID", customMsg.TxID),
				zap.Error(err),
			)
			return nil, fmt.Errorf("failed to parse transaction ID: %w", err)
		}

		messages = append(messages, &relayerTypes.WarpMessageInfo{
			SourceAddress:   sourceAddr,
			SourceTxID:      txID,
			UnsignedMessage: unsignedMsg,
		})

		s.logger.Debug(
			"Successfully converted warp message",
			zap.Int("messageIndex", i),
			zap.String("sourceAddress", fmt.Sprintf("%x", sourceAddr)),
			zap.String("txID", fmt.Sprintf("%x", txID)),
		)
	}

	s.logger.Debug(
		"Successfully converted all warp messages for block",
		zap.Uint64("blockNumber", customBlock.Height),
		zap.Int("totalMessages", len(messages)),
		zap.Int("originalMessageCount", len(customBlock.Messages)),
	)

	// If all messages were skipped (empty unsigned messages), return nil to skip this block
	if len(messages) == 0 {
		s.logger.Debug(
			"No valid warp messages in block after filtering",
			zap.Uint64("blockNumber", customBlock.Height),
		)
		return nil, nil
	}

	return &relayerTypes.WarpBlockInfo{
		BlockNumber: customBlock.Height,
		Messages:    messages,
	}, nil
}

// parseUnsignedMessage parses the base64-encoded unsigned warp message
func (s *Subscriber) parseUnsignedMessage(base64Message string) (*avalancheWarp.UnsignedMessage, error) {
	// Decode from base64 (standard encoding)
	msgBytes, err := base64.StdEncoding.DecodeString(base64Message)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 message: %w", err)
	}

	if len(msgBytes) == 0 {
		return nil, fmt.Errorf("decoded message is empty")
	}

	s.logger.Debug(
		"Decoded base64 unsigned message",
		zap.Int("messageLength", len(msgBytes)),
		zap.String("messageHex", common.Bytes2Hex(msgBytes[:min(32, len(msgBytes))])),
	)

	// Parse as avalanche warp message
	unsignedMsg, err := avalancheWarp.ParseUnsignedMessage(msgBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse avalanche warp message: %w", err)
	}

	return unsignedMsg, nil
}

// extractSourceAddressFromPayload extracts the source contract address from a warp message payload
// For Teleporter messages (AddressedCall), this extracts the source address from the payload
func (s *Subscriber) extractSourceAddressFromPayload(unsignedMsg *avalancheWarp.UnsignedMessage) ([20]byte, error) {
	var addr [20]byte

	// Try to parse as AddressedCall (used by Teleporter)
	addressedPayload, err := warpPayload.ParseAddressedCall(unsignedMsg.Payload)
	if err != nil {
		s.logger.Debug(
			"Failed to parse payload as AddressedCall, using zero address",
			zap.Error(err),
		)
		// If not an AddressedCall, return zero address (will be rejected by message coordinator)
		return addr, nil
	}

	s.logger.Debug(
		"Successfully parsed payload as AddressedCall",
		zap.String("sourceAddress", fmt.Sprintf("0x%x", addressedPayload.SourceAddress)),
	)

	// The source address is the SourceAddress field in the AddressedCall
	// This is the Teleporter contract address
	if len(addressedPayload.SourceAddress) != 20 {
		return addr, fmt.Errorf("source address must be 20 bytes, got %d", len(addressedPayload.SourceAddress))
	}

	copy(addr[:], addressedPayload.SourceAddress)

	s.logger.Debug(
		"Extracted source address from AddressedCall payload",
		zap.String("sourceAddress", fmt.Sprintf("0x%x", addr)),
	)

	return addr, nil
}

// parseSourceAddress converts custom VM address to common.Address
func (s *Subscriber) parseSourceAddress(address string) ([20]byte, error) {
	var addr [20]byte

	// For custom VMs, we'll try to convert the address to a 20-byte array
	// If the address is hex-encoded, use it directly
	if len(address) >= 2 && address[:2] == "0x" {
		addressBytes := common.FromHex(address)
		if len(addressBytes) != 20 {
			return addr, fmt.Errorf("address must be 20 bytes, got %d", len(addressBytes))
		}
		copy(addr[:], addressBytes)
		s.logger.Debug(
			"Parsed hex source address",
			zap.String("originalAddress", address),
			zap.String("parsedAddress", fmt.Sprintf("0x%x", addr)),
		)
		return addr, nil
	}

	// Try to decode as base64 (Custom VMs may return base64-encoded addresses)
	addressBytes, err := base64.StdEncoding.DecodeString(address)
	if err == nil && len(addressBytes) == 20 {
		// Successfully decoded as base64 and it's 20 bytes
		copy(addr[:], addressBytes)
		s.logger.Debug(
			"Parsed base64 source address",
			zap.String("originalAddress", address),
			zap.String("parsedAddress", fmt.Sprintf("0x%x", addr)),
		)
		return addr, nil
	}

	// For non-hex/non-base64 addresses, we'll hash them to get a 20-byte representation
	addressBytes = []byte(address)
	if len(addressBytes) > 20 {
		// Use the first 20 bytes of a hash if the address is too long
		hash := crypto.Keccak256(addressBytes)
		copy(addr[:], hash[:20])
	} else {
		// Pad with zeros if the address is too short
		copy(addr[:], addressBytes)
	}

	s.logger.Debug(
		"Parsed source address (fallback)",
		zap.String("originalAddress", address),
		zap.String("parsedAddress", fmt.Sprintf("0x%x", addr)),
	)

	return addr, nil
}

// parseTxID converts custom VM transaction ID to common.Hash
func (s *Subscriber) parseTxID(txID string) ([32]byte, error) {
	var hash [32]byte

	// Handle empty transaction ID
	if txID == "" {
		s.logger.Debug("Empty transaction ID, using zero hash")
		return hash, nil
	}

	// Try to decode as CB58 (Avalanche's standard encoding for IDs)
	// Example: R6EWGcNJURnJv9qGnZYFrkYBnTcX9ShAaxrZjBHTiXfkGJKxR
	txIDObj, err := ids.FromString(txID)
	if err == nil {
		s.logger.Debug(
			"Successfully decoded CB58 transaction ID",
			zap.String("txID", txID),
			zap.String("decoded", txIDObj.String()),
		)
		copy(hash[:], txIDObj[:])
		return hash, nil
	}

	// If CB58 decoding failed, try hex format
	cleanTxID := txID
	if len(txID) >= 2 && txID[:2] == "0x" {
		cleanTxID = txID[2:]
	}

	// Convert hex to bytes
	txBytes := common.FromHex(cleanTxID)

	// If conversion failed or empty, hash the original string as fallback
	if len(txBytes) == 0 {
		s.logger.Debug(
			"Transaction ID is not CB58 or hex, hashing it",
			zap.String("txID", txID),
		)
		// Hash the transaction ID string to get a 32-byte representation
		hashBytes := crypto.Keccak256([]byte(txID))
		copy(hash[:], hashBytes)
		return hash, nil
	}

	// If it's exactly 32 bytes, use it directly
	if len(txBytes) == 32 {
		copy(hash[:], txBytes)
		return hash, nil
	}

	// If it's shorter than 32 bytes, pad with zeros on the left
	if len(txBytes) < 32 {
		s.logger.Debug(
			"Transaction ID is shorter than 32 bytes, padding with zeros",
			zap.String("txID", txID),
			zap.Int("actualBytes", len(txBytes)),
		)
		copy(hash[32-len(txBytes):], txBytes)
		return hash, nil
	}

	// If it's longer than 32 bytes, use the first 32 bytes
	s.logger.Debug(
		"Transaction ID is longer than 32 bytes, truncating",
		zap.String("txID", txID),
		zap.Int("actualBytes", len(txBytes)),
	)
	copy(hash[:], txBytes[:32])
	return hash, nil
}

// GetLatestBlockHeight fetches the latest block height from a custom VM via JSON-RPC
// This is a utility function that can be called from outside the package
func GetLatestBlockHeight(baseURL string) (uint64, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "warpcustomvm.getLatestBlock",
		"params":  map[string]interface{}{},
		"id":      1,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal JSON-RPC request: %w", err)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Post(
		baseURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to call JSON-RPC: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP error %d when calling JSON-RPC", resp.StatusCode)
	}

	var result struct {
		Result struct {
			Height uint64 `json:"height"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode JSON-RPC response: %w", err)
	}

	if result.Error != nil {
		return 0, fmt.Errorf("JSON-RPC error: %s (code: %d)", result.Error.Message, result.Error.Code)
	}

	return result.Result.Height, nil
}
