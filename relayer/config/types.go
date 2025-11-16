// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package config

// Supported VMs
type VM int

const (
	UNKNOWN_VM VM = iota
	EVM
	CUSTOM
)

func (vm VM) String() string {
	switch vm {
	case EVM:
		return "evm"
	case CUSTOM:
		return "custom"
	default:
		return "unknown"
	}
}

// ParseVM returns the VM corresponding to [vm]
func ParseVM(vm string) VM {
	switch vm {
	case "evm":
		return EVM
	case "custom":
		return CUSTOM
	default:
		return UNKNOWN_VM
	}
}

// Supported Message Protocols
type MessageProtocol int

const (
	UNKNOWN_MESSAGE_PROTOCOL MessageProtocol = iota
	TELEPORTER
	OFF_CHAIN_REGISTRY
	RAW
)

func (msg MessageProtocol) String() string {
	switch msg {
	case TELEPORTER:
		return "teleporter"
	case OFF_CHAIN_REGISTRY:
		return "off-chain-registry"
	case RAW:
		return "raw"
	default:
		return "unknown"
	}
}

// ParseMessageProtocol returns the MessageProtocol corresponding to [msg]
func ParseMessageProtocol(msg string) MessageProtocol {
	switch msg {
	case "teleporter":
		return TELEPORTER
	case "off-chain-registry":
		return OFF_CHAIN_REGISTRY
	case "raw":
		return RAW
	default:
		return UNKNOWN_MESSAGE_PROTOCOL
	}
}
