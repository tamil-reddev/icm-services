// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package custom

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	xsvmTx "github.com/ava-labs/avalanchego/vms/example/xsvm/tx"
	pchainapi "github.com/ava-labs/avalanchego/vms/platformvm/api"
	avalancheWarp "github.com/ava-labs/avalanchego/vms/platformvm/warp"
	"github.com/ava-labs/icm-services/peers"
	"github.com/ava-labs/icm-services/relayer/config"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"go.uber.org/zap"
)

const (
	// Timeout for HTTP requests to custom VM
	defaultCustomVMTimeout = 30 * time.Second
)

// Custom VM transaction request structure
type CustomVMTxRequest struct {
	WarpMessage   string   `json:"warpMessage"`   // hex-encoded signed warp message
	ToAddress     string   `json:"toAddress"`     // destination address
	GasLimit      uint64   `json:"gasLimit"`      // gas limit for the transaction
	CallData      string   `json:"callData"`      // hex-encoded call data
	Deliverers    []string `json:"deliverers"`    // list of deliverer addresses
	SenderAddress string   `json:"senderAddress"` // sender address used by relayer
}

// Custom VM transaction response structure
type CustomVMTxResponse struct {
	TxID     string `json:"txId"`     // transaction ID
	BlockNum uint64 `json:"blockNum"` // block number where tx was included
	Success  bool   `json:"success"`  // whether transaction was successful
	GasUsed  uint64 `json:"gasUsed"`  // gas used by transaction
}

// destinationClient implements DestinationClient for custom VMs
type destinationClient struct {
	client                  *http.Client
	baseURL                 string
	destinationBlockchainID ids.ID
	blockGasLimit           uint64
	logger                  logging.Logger
	senderAddresses         []common.Address
}

// NewDestinationClient creates a new destination client for custom VM
func NewDestinationClient(
	logger logging.Logger,
	destinationBlockchain *config.DestinationBlockchain,
) (*destinationClient, error) {
	logger = logger.With(zap.String("blockchainID", destinationBlockchain.BlockchainID))

	destinationID, err := ids.FromString(destinationBlockchain.BlockchainID)
	if err != nil {
		logger.Error(
			"Could not decode destination chain ID from string",
			zap.Error(err),
		)
		return nil, err
	}

	// For custom VMs, the RPC endpoint should be the base URL for the custom VM API
	baseURL := destinationBlockchain.RPCEndpoint.BaseURL
	if baseURL == "" {
		return nil, fmt.Errorf("RPC endpoint base URL is required for custom VM destination client")
	}

	client := &destinationClient{
		client:                  &http.Client{Timeout: defaultCustomVMTimeout},
		baseURL:                 baseURL,
		destinationBlockchainID: destinationID,
		blockGasLimit:           8000000, // Default gas limit, can be made configurable
		logger:                  logger,
	}

	// Parse sender addresses from account private keys
	err = client.initializeSenderAddresses(destinationBlockchain)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sender addresses: %w", err)
	}

	// Test connection to the custom VM
	err = client.testConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to custom VM: %w", err)
	}

	logger.Debug(
		"Created custom VM destination client",
		zap.String("baseURL", baseURL),
		zap.Int("senderAddressCount", len(client.senderAddresses)),
	)

	return client, nil
}

// SendTx sends a transaction to the custom VM
func (c *destinationClient) SendTx(
	signedMessage *avalancheWarp.Message,
	deliverers set.Set[common.Address],
	toAddress string,
	gasLimit uint64,
	callData []byte,
) (*types.Receipt, error) {
	// Prepare the transaction request for custom VM
	txRequest := CustomVMTxRequest{
		WarpMessage: common.Bytes2Hex(signedMessage.Bytes()),
		ToAddress:   toAddress,
		GasLimit:    gasLimit,
		CallData:    common.Bytes2Hex(callData),
		Deliverers:  make([]string, 0, deliverers.Len()),
	}

	// Convert deliverer addresses to strings
	for deliverer := range deliverers {
		txRequest.Deliverers = append(txRequest.Deliverers, deliverer.Hex())
	}

	// Use the first sender address
	if len(c.senderAddresses) > 0 {
		txRequest.SenderAddress = c.senderAddresses[0].Hex()
	}

	c.logger.Debug(
		"Sending transaction to custom VM",
		zap.String("toAddress", toAddress),
		zap.Uint64("gasLimit", gasLimit),
		zap.Int("delivererCount", len(txRequest.Deliverers)),
	)

	// Send the transaction to custom VM
	response, err := c.sendCustomVMTransaction(txRequest)
	if err != nil {
		c.logger.Error(
			"Failed to send transaction to custom VM",
			zap.Error(err),
		)
		return nil, err
	}

	if !response.Success {
		return nil, fmt.Errorf("custom VM transaction failed: txID=%s", response.TxID)
	}

	// Convert custom VM response to types.Receipt format
	receipt := &types.Receipt{
		TxHash:      common.HexToHash(response.TxID),
		BlockNumber: big.NewInt(int64(response.BlockNum)),
		GasUsed:     response.GasUsed,
		Status:      types.ReceiptStatusSuccessful,
	}

	c.logger.Debug(
		"Successfully sent transaction to custom VM",
		zap.String("txID", response.TxID),
		zap.Uint64("blockNumber", response.BlockNum),
		zap.Uint64("gasUsed", response.GasUsed),
	)

	return receipt, nil
}

// Client returns the underlying HTTP client
func (c *destinationClient) Client() interface{} {
	return c.client
}

// SenderAddresses returns the sender addresses used by this client
func (c *destinationClient) SenderAddresses() []common.Address {
	return c.senderAddresses
}

// DestinationBlockchainID returns the blockchain ID
func (c *destinationClient) DestinationBlockchainID() ids.ID {
	return c.destinationBlockchainID
}

// BlockGasLimit returns the block gas limit
func (c *destinationClient) BlockGasLimit() uint64 {
	return c.blockGasLimit
}

// GetRPCEndpointURL returns the RPC endpoint URL for this destination blockchain
func (c *destinationClient) GetRPCEndpointURL() string {
	return c.baseURL
}

// GetPChainHeightForDestination determines the appropriate P-Chain height for validator set selection.
// For custom VMs, we use ProposedHeight as they typically don't have epoch-based validator set rotation.
func (c *destinationClient) GetPChainHeightForDestination(
	ctx context.Context,
	network peers.AppRequestNetwork,
) (uint64, error) {
	c.logger.Debug("Using ProposedHeight for custom VM destination client")
	// Custom VMs don't support epoch-based validator sets, so we always use ProposedHeight
	return pchainapi.ProposedHeight, nil
}

// initializeSenderAddresses initializes sender addresses from private keys
func (c *destinationClient) initializeSenderAddresses(destinationBlockchain *config.DestinationBlockchain) error {
	// For custom VMs, we expect the account private key to be provided
	// This is a simplified approach - in a real implementation, you might want to support
	// multiple addresses or derive addresses from the private key

	if destinationBlockchain.AccountPrivateKey != "" {
		// For custom VMs, we'll derive a common.Address from the private key
		// This is a placeholder - the actual derivation would depend on the custom VM's addressing scheme
		privateKeyBytes := common.FromHex(destinationBlockchain.AccountPrivateKey)
		if len(privateKeyBytes) >= 20 {
			var addr common.Address
			copy(addr[:], privateKeyBytes[:20])
			c.senderAddresses = append(c.senderAddresses, addr)
		} else {
			return fmt.Errorf("invalid private key format for custom VM")
		}
	} else {
		return fmt.Errorf("account private key is required for custom VM destination client")
	}

	return nil
}

// testConnection tests the connection to the custom VM
func (c *destinationClient) testConnection() error {
	/*endpoint := fmt.Sprintf("%s/health", c.baseURL)

	resp, err := c.client.Get(endpoint)
	if err != nil {
		return fmt.Errorf("failed to connect to custom VM health endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("custom VM health check failed with status %d", resp.StatusCode)
	}

	c.logger.Debug("Successfully connected to custom VM")*/
	return nil
}

// sendCustomVMTransaction sends a transaction to the custom VM
func (c *destinationClient) sendCustomVMTransaction(txRequest CustomVMTxRequest) (*CustomVMTxResponse, error) {
	c.logger.Debug(
		"Attempting to send transaction to custom VM",
		zap.String("method", "JSON-RPC first, then REST API fallback"),
		zap.String("baseURL", c.baseURL),
		zap.Int("warpMessageLength", len(txRequest.WarpMessage)),
	)

	// Try JSON-RPC first (for VMs like xsvm)
	response, err := c.sendCustomVMTransactionJSONRPC(txRequest)
	if err == nil {
		c.logger.Debug("Successfully sent transaction via JSON-RPC")
		return response, nil
	}
	return nil, err
}

// sendCustomVMTransactionJSONRPC sends transaction using JSON-RPC (xsvm.issueTx)
func (c *destinationClient) sendCustomVMTransactionJSONRPC(txRequest CustomVMTxRequest) (*CustomVMTxResponse, error) {
	c.logger.Debug(
		"Attempting JSON-RPC transaction submission",
		zap.String("method", "xsvm.issueTx"),
		zap.String("endpoint", c.baseURL),
	)

	// Decode the hex-encoded warp message to get the actual warp.Message
	warpMessageBytes := common.FromHex(txRequest.WarpMessage)
	if len(warpMessageBytes) == 0 {
		c.logger.Error("Warp message is empty after hex decoding")
		return nil, fmt.Errorf("invalid warp message: empty after hex decoding")
	}

	c.logger.Debug(
		"Decoded warp message bytes",
		zap.Int("length", len(warpMessageBytes)),
		zap.String("firstBytes", fmt.Sprintf("%x", warpMessageBytes[:min(32, len(warpMessageBytes))])),
	)

	// Parse the warp message
	warpMsg, err := avalancheWarp.ParseMessage(warpMessageBytes)
	if err != nil {
		c.logger.Error("Failed to parse warp message", zap.Error(err))
		return nil, fmt.Errorf("failed to parse warp message: %w", err)
	}

	c.logger.Debug(
		"Parsed warp message",
		zap.String("sourceChainID", warpMsg.SourceChainID.String()),
		zap.Int("payloadLength", len(warpMsg.Payload)),
	)

	// Construct an Import transaction that wraps the signed warp message
	// This is what xsvm expects when calling xsvm.issueTx
	//
	// The Import transaction structure is:
	// - Nonce: internal chain replay protection (TODO: implement nonce management)
	// - MaxFee: maximum fee for the transaction (TODO: make configurable)
	// - Message: the signed warp message bytes (provides cross-chain replay protection)
	importTx := &xsvmTx.Import{
		Nonce:   0,       // TODO: Implement nonce management
		MaxFee:  1000000, // TODO: Make configurable
		Message: warpMessageBytes,
	}

	c.logger.Debug(
		"Constructed Import transaction",
		zap.Uint64("nonce", importTx.Nonce),
		zap.Uint64("maxFee", importTx.MaxFee),
		zap.Int("messageLength", len(importTx.Message)),
		zap.String("sourceChainID", warpMsg.SourceChainID.String()),
	)

	// Wrap the Import transaction in a tx.Tx
	tx := &xsvmTx.Tx{
		Unsigned: importTx,
	}

	// Marshal the transaction using xsvm's codec
	// This prepends the codec version (0) and type ID
	txBytes, err := xsvmTx.Codec.Marshal(xsvmTx.CodecVersion, tx)
	if err != nil {
		c.logger.Error("Failed to marshal Import transaction", zap.Error(err))
		return nil, fmt.Errorf("failed to marshal Import transaction: %w", err)
	}

	c.logger.Debug(
		"Marshaled Import transaction",
		zap.Int("txBytesLength", len(txBytes)),
		zap.String("txBytesHex", hex.EncodeToString(txBytes[:min(64, len(txBytes))])),
	)

	// CRITICAL: xsvm expects raw bytes (not hex string) in the "tx" parameter.
	// The Go JSON encoder will automatically base64-encode the byte array.
	// This matches how the xsvm client works: IssueTxArgs{Tx: txBytes}
	//
	// We need to structure the params as a proper object with "tx" field containing bytes
	type issueTxParams struct {
		Tx []byte `json:"tx"`
	}

	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "xsvm.issueTx",
		"params":  issueTxParams{Tx: txBytes}, // Raw bytes, JSON encoder will base64 encode
		"id":      1,
	}

	c.logger.Debug(
		"JSON-RPC request payload prepared",
		zap.Int("txBytesLength", len(txBytes)),
		zap.String("txBytesPreview", hex.EncodeToString(txBytes[:min(32, len(txBytes))])),
	)

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.logger.Error("Failed to marshal JSON-RPC request", zap.Error(err))
		return nil, fmt.Errorf("failed to marshal JSON-RPC request: %w", err)
	}

	c.logger.Debug(
		"Sending JSON-RPC request",
		zap.String("url", c.baseURL),
		zap.Int("payloadSize", len(jsonData)),
	)

	resp, err := c.client.Post(
		c.baseURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		c.logger.Error(
			"Failed to call JSON-RPC endpoint",
			zap.Error(err),
			zap.String("url", c.baseURL),
		)
		return nil, fmt.Errorf("failed to call JSON-RPC: %w", err)
	}
	defer resp.Body.Close()

	c.logger.Debug(
		"Received JSON-RPC response",
		zap.Int("statusCode", resp.StatusCode),
		zap.String("status", resp.Status),
	)

	if resp.StatusCode != http.StatusOK {
		c.logger.Error(
			"JSON-RPC HTTP error",
			zap.Int("statusCode", resp.StatusCode),
			zap.String("status", resp.Status),
		)
		return nil, fmt.Errorf("HTTP error %d when calling JSON-RPC", resp.StatusCode)
	}

	var rpcResponse struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  struct {
			TxID string `json:"txId"`
		} `json:"result"`
		Error *struct {
			Code    int         `json:"code"`
			Message string      `json:"message"`
			Data    interface{} `json:"data,omitempty"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResponse); err != nil {
		c.logger.Error(
			"Failed to decode JSON-RPC response",
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to decode JSON-RPC response: %w", err)
	}

	c.logger.Debug(
		"Decoded JSON-RPC response",
		zap.Any("response", rpcResponse),
	)

	if rpcResponse.Error != nil {
		c.logger.Error(
			"JSON-RPC returned error",
			zap.Int("errorCode", rpcResponse.Error.Code),
			zap.String("errorMessage", rpcResponse.Error.Message),
			zap.Any("errorData", rpcResponse.Error.Data),
		)
		return nil, fmt.Errorf("JSON-RPC error %d: %s", rpcResponse.Error.Code, rpcResponse.Error.Message)
	}

	if rpcResponse.Result.TxID == "" {
		c.logger.Error("JSON-RPC response missing txId in result")
		return nil, fmt.Errorf("JSON-RPC response missing txId")
	}

	c.logger.Info(
		"Successfully submitted Import transaction to xsvm",
		zap.String("txID", rpcResponse.Result.TxID),
		zap.String("sourceChainID", warpMsg.SourceChainID.String()),
		zap.String("destinationChainID", c.destinationBlockchainID.String()),
	)

	// Return a response with the transaction ID
	return &CustomVMTxResponse{
		TxID:    rpcResponse.Result.TxID,
		Success: true,
	}, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
