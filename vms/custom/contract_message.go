// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package custom

import (
	"fmt"

	"github.com/ava-labs/avalanchego/utils/logging"
	avalancheWarp "github.com/ava-labs/avalanchego/vms/platformvm/warp"
	"github.com/ava-labs/icm-services/relayer/config"
	"github.com/ava-labs/libevm/common"
	"go.uber.org/zap"
)

// contractMessage implements ContractMessage interface for custom VMs
type contractMessage struct {
	logger logging.Logger
}

// NewContractMessage creates a new contract message handler for custom VMs
func NewContractMessage(logger logging.Logger, subnetInfo config.SourceBlockchain) *contractMessage {
	return &contractMessage{
		logger: logger,
	}
}

// UnpackWarpMessage unpacks the warp message from the custom VM
// For custom VMs, we expect the unsigned message bytes to be directly parseable
// as an avalanche warp UnsignedMessage
func (c *contractMessage) UnpackWarpMessage(unsignedMsgBytes []byte) (*avalancheWarp.UnsignedMessage, error) {
	c.logger.Debug(
		"Unpacking warp message for custom VM",
		zap.Int("messageLength", len(unsignedMsgBytes)),
	)

	// For custom VMs, we try to parse the message bytes directly as an avalanche warp message
	unsignedMessage, err := avalancheWarp.ParseUnsignedMessage(unsignedMsgBytes)
	if err != nil {
		c.logger.Error(
			"Failed to parse unsigned warp message for custom VM",
			zap.Error(err),
			zap.Int("messageLength", len(unsignedMsgBytes)),
			zap.String("messageHex", common.Bytes2Hex(unsignedMsgBytes)),
		)
		return nil, fmt.Errorf("failed to parse custom VM warp message: %w", err)
	}

	c.logger.Debug(
		"Successfully unpacked custom VM warp message",
		zap.String("sourceChainID", unsignedMessage.SourceChainID.String()),
		zap.String("messageID", unsignedMessage.ID().String()),
	)

	return unsignedMessage, nil
}
