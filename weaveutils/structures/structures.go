package structures

import (
	fountain "github.com/Watchdog-Network/gofountain"
)

type ACK struct {
	AckMessage []byte `json:"ack"`
	Id         []byte `json:"ID"`
}

// Channel
// Id means the identifier of the peer
// Symbols
// Hash
// Length
type BlockData struct {
	Channel                 string
	Id                      string
	Symbols                 []SymbolData
	Length                  int
	Hash                    [32]byte
	NumSourceSymbols        int
	SymbolAlignmentSize     int
	NumEncodedSourceSymbols int
}

// Id means the identifier of the peer
// SourceData indicates the encoding symbols of the block data
// Length means that the size of the encoding symbols
// Hash accommodates 32-byte length hash value that is from the block data using SHA-256
type SymbolData struct {
	Id                      string
	SourceData              fountain.LTBlock `json:"sourcedata"`
	Length                  int              `json:"length"`
	Hash                    [32]byte         `json:"hash"`
	NumSourceSymbols        int              `json:"numsourcesymbols"`
	SymbolAlignmentSize     int              `json:"symbolalignmentsize"`
	NumEncodedSourceSymbols int              `json:"numencodedsourcesymbols"`
}
