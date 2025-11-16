// Copyright (C) 2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package raw

import (
	"encoding/json"
	"fmt"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/vms/platformvm/warp"
	"github.com/ava-labs/icm-services/messages"
	"github.com/ava-labs/icm-services/relayer/config"
	"github.com/ava-labs/icm-services/vms"
	"github.com/ava-labs/libevm/common"
	"go.uber.org/zap"
)

// RawMessageHandler handles warp messages with raw payloads (no structured format)
// This is suitable for custom VMs that don't use Teleporter or AddressedCall formats

type factory struct {
	logger             logging.Logger
	destinationChainID string
	destinationAddress common.Address
}

type messageHandler struct {
	logger            logging.Logger
	unsignedMessage   *warp.UnsignedMessage
	destinationClient vms.DestinationClient
	logFields         []zap.Field
}

type RawMessageConfig struct {
	DestinationBlockchainID string `json:"destination-blockchain-id" mapstructure:"destination-blockchain-id"`
	DestinationAddress      string `json:"destination-address" mapstructure:"destination-address"`
}

func NewMessageHandlerFactory(
	logger logging.Logger,
	messageProtocolConfig config.MessageProtocolConfig,
) (messages.MessageHandlerFactory, error) {
	// Parse the raw message config
	data, err := json.Marshal(messageProtocolConfig.Settings)
	if err != nil {
		logger.Error("Failed to marshal raw message config", zap.Error(err))
		return nil, err
	}

	var cfg RawMessageConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		logger.Error("Failed to unmarshal raw message config", zap.Error(err))
		return nil, err
	}

	// Validate config
	if cfg.DestinationBlockchainID == "" {
		return nil, fmt.Errorf("destination-blockchain-id is required for raw message handler")
	}

	destAddress := common.HexToAddress(cfg.DestinationAddress)
	if destAddress == (common.Address{}) {
		logger.Warn("No destination address specified, using zero address")
	}

	logger.Debug(
		"Creating raw message handler factory",
		zap.String("destinationBlockchainID", cfg.DestinationBlockchainID),
		zap.String("destinationAddress", destAddress.Hex()),
	)

	return &factory{
		logger:             logger,
		destinationChainID: cfg.DestinationBlockchainID,
		destinationAddress: destAddress,
	}, nil
}

func (f *factory) GetMessageRoutingInfo(unsignedMessage *warp.UnsignedMessage) (
	messages.MessageRoutingInfo,
	error,
) {
	// For raw messages, we don't parse the payload - just pass it through
	// The source address comes from the warp message itself
	f.logger.Debug(
		"Getting routing info for raw warp message",
		zap.Stringer("warpMessageID", unsignedMessage.ID()),
		zap.Stringer("sourceChainID", unsignedMessage.SourceChainID),
		zap.Int("payloadLength", len(unsignedMessage.Payload)),
	)

	// For raw messages, we use a convention where the source address
	// is derived from the source chain ID or set to a default value
	sourceAddress := common.BytesToAddress(unsignedMessage.SourceChainID[:])

	// Parse destination blockchain ID
	destinationChainID, err := parseBlockchainID(f.destinationChainID)
	if err != nil {
		f.logger.Error("Failed to parse destination blockchain ID", zap.Error(err))
		return messages.MessageRoutingInfo{}, err
	}

	return messages.MessageRoutingInfo{
		SourceChainID:      unsignedMessage.SourceChainID,
		SenderAddress:      sourceAddress,
		DestinationChainID: destinationChainID,
		DestinationAddress: f.destinationAddress,
	}, nil
}

func (f *factory) NewMessageHandler(
	unsignedMessage *warp.UnsignedMessage,
	destinationClient vms.DestinationClient,
) (messages.MessageHandler, error) {
	return &messageHandler{
		logger:            f.logger,
		unsignedMessage:   unsignedMessage,
		destinationClient: destinationClient,
		logFields: []zap.Field{
			zap.Stringer("warpMessageID", unsignedMessage.ID()),
			zap.Stringer("sourceChainID", unsignedMessage.SourceChainID),
		},
	}, nil
}

func (m *messageHandler) GetUnsignedMessage() *warp.UnsignedMessage {
	return m.unsignedMessage
}

func (m *messageHandler) ShouldSendMessage() (bool, error) {
	// For raw messages, always send (no duplicate checking)
	m.logger.Debug("Raw message handler - always sending message", m.logFields...)
	return true, nil
}

func (m *messageHandler) SendMessage(signedMessage *warp.Message, isGraniteActivated bool) (common.Hash, error) {
	m.logger.Debug(
		"Sending raw warp message to destination",
		append(m.logFields,
			zap.Int("signedMessageLength", len(signedMessage.Bytes())),
			zap.Int("payloadLength", len(m.unsignedMessage.Payload)),
			zap.Bool("isGraniteActivated", isGraniteActivated),
		)...,
	)

	// Send the signed warp message directly to the destination
	// For raw messages, we send to zero address with no call data
	// The custom VM will handle the warp message directly
	receipt, err := m.destinationClient.SendTx(
		signedMessage,
		set.Set[common.Address]{},                    // Empty set of deliverers
		"0x0000000000000000000000000000000000000000", // Zero address for raw messages
		0,   // No gas limit for custom VMs
		nil, // No call data needed for raw messages
	)
	if err != nil {
		m.logger.Error(
			"Failed to send raw warp message",
			append(m.logFields, zap.Error(err))...,
		)
		return common.Hash{}, err
	}

	m.logger.Debug(
		"Successfully sent raw warp message",
		append(m.logFields,
			zap.String("txHash", receipt.TxHash.Hex()),
		)...,
	)

	return receipt.TxHash, nil
}

func (m *messageHandler) LoggerWithContext(logger logging.Logger) logging.Logger {
	return logger.With(m.logFields...)
}

// parseBlockchainID converts a blockchain ID string to [32]byte
func parseBlockchainID(blockchainIDStr string) ([32]byte, error) {
	var blockchainID [32]byte

	// Try to parse as hex (0x-prefixed)
	if len(blockchainIDStr) >= 2 && blockchainIDStr[:2] == "0x" {
		decoded := common.FromHex(blockchainIDStr)
		if len(decoded) != 32 {
			return blockchainID, fmt.Errorf("invalid blockchain ID hex length: expected 32 bytes, got %d", len(decoded))
		}
		copy(blockchainID[:], decoded)
		return blockchainID, nil
	}

	// Try to parse as CB58 (Avalanche format)
	id, err := ids.FromString(blockchainIDStr)
	if err != nil {
		return blockchainID, fmt.Errorf("invalid blockchain ID format (not hex or CB58): %w", err)
	}

	return [32]byte(id), nil
}
