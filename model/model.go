package model

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/wire"
)

type Block struct {
	Hash     string
	Height   uint64
	IsOrphan bool

	PreviousBlock string
	Version       int32
	Nonce         uint32
	Timestamp     time.Time
	Bits          uint32
	MerkleRoot    string

	//Transactions
	Txs []string
}

type Transaction struct {
	Hash     string
	LockTime uint32
	Version  int32
	Safe     bool

	BlockHash string

	Vins  []Vin
	Vouts []Vout
}

func (t *Transaction) ToWireTx() (*wire.MsgTx, error) {
	wireTx := &wire.MsgTx{}
	for _, vin := range t.Vins {
		witnessBytes, err := vin.DecodeWitness()
		if err != nil {
			return nil, err
		}
		wireTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{},
			SignatureScript:  []byte(vin.SignatureScript),
			Sequence:         vin.Sequence,
			Witness:          witnessBytes,
		})
	}
	for _, vout := range t.Vouts {
		pkScript, _ := hex.DecodeString(vout.ScriptPubKey)
		wireTx.AddTxOut(&wire.TxOut{
			Value:    vout.Value,
			PkScript: pkScript,
		})
	}
	return wireTx, nil
}

func UnmarshalTxRawResult(data *btcjson.TxRawResult) (*Transaction, error) {
	tx := &Transaction{
		Hash:     data.Txid,
		LockTime: data.LockTime,
		Version:  int32(data.Version),
	}
	for _, vin := range data.Vin {
		if len(vin.Witness) == 0 {
			vin.Witness = []string{""}
		}
		tx.Vins = append(tx.Vins, Vin{
			TxId:            vin.Txid,
			Index:           vin.Vout,
			Sequence:        vin.Sequence,
			SignatureScript: vin.ScriptSig.Hex,
			Witness:         vin.Witness[0],
		})
	}
	for _, vout := range data.Vout {
		tx.Vouts = append(tx.Vouts, Vout{
			TxId:         data.Txid,
			Index:        uint32(vout.N),
			ScriptPubKey: vout.ScriptPubKey.Hex,
			Value:        int64(vout.Value),
			Type:         vout.ScriptPubKey.Type,
		})
	}
	return tx, nil
}

type Vin struct {
	TxId            string
	Index           uint32
	Sequence        uint32
	SignatureScript string
	Witness         string
}

func (v *Vin) DecodeWitness() ([][]byte, error) {
	splits := strings.Split(v.Witness, ",")
	var witness [][]byte
	for _, s := range splits {
		w, err := hex.DecodeString(s)
		if err != nil {
			return nil, err
		}
		witness = append(witness, w)
	}
	return witness, nil
}

func EncodeWitnesss(witness [][]byte) string {
	var witnessStrings []string
	for _, w := range witness {
		witnessStrings = append(witnessStrings, hex.EncodeToString(w))
	}
	return strings.Join(witnessStrings, ",")
}

type Vout struct {
	TxId         string
	Index        uint32
	ScriptPubKey string
	Value        int64
	Type         string
}

func UnmarshalBlock(data []byte) (*Block, error) {
	block := &Block{}
	err := json.Unmarshal(data, block)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (b *Block) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

func UnmarshalVout(data []byte) (*Vout, error) {
	vout := &Vout{}
	err := json.Unmarshal(data, vout)
	if err != nil {
		return nil, err
	}
	return vout, nil
}

func UnmarshalVouts(data []byte) ([]*Vout, error) {
	var vouts []*Vout
	err := json.Unmarshal(data, &vouts)
	if err != nil {
		return nil, err
	}
	return vouts, nil
}

func MarshalVout(vout Vout) []byte {
	data, _ := json.Marshal(vout)
	return data
}

func (v *Vout) Marshal() []byte {
	data, _ := json.Marshal(v)
	return data
}

func (t *Transaction) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func UnmarshalTransaction(data []byte) (*Transaction, error) {
	tx := &Transaction{}
	err := json.Unmarshal(data, tx)
	if err != nil {
		return nil, err
	}
	return tx, nil
}
