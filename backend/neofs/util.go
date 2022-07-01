package neofs

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	resolver "github.com/nspcc-dev/neofs-sdk-go/ns"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/rclone/rclone/fs"
)

func createPool(ctx context.Context, key *keys.PrivateKey, cfg *Options) (*pool.Pool, error) {
	var prm pool.InitParameters
	prm.SetKey(&key.PrivateKey)
	prm.SetNodeDialTimeout(time.Duration(cfg.NeofsConnectionTimeout))
	prm.SetHealthcheckTimeout(time.Duration(cfg.NeofsRequestTimeout))
	prm.SetClientRebalanceInterval(time.Duration(cfg.NeofsRebalanceInterval))
	prm.SetSessionExpirationDuration(cfg.NeofsSessionExpirationDuration)
	prm.AddNode(pool.NewNodeParam(1, cfg.NeofsEndpoint, 1))

	p, err := pool.NewPool(prm)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err = p.Dial(ctx); err != nil {
		return nil, fmt.Errorf("dial pool: %w", err)
	}

	return p, nil
}

func createNNSResolver(cfg *Options) (*resolver.NNS, error) {
	if cfg.RpcEndpoint == "" {
		return nil, nil
	}

	var nns resolver.NNS
	if err := nns.Dial(cfg.RpcEndpoint); err != nil {
		return nil, fmt.Errorf("dial NNS resolver: %w", err)
	}

	return &nns, nil
}

func getAccount(cfg *Options) (*wallet.Account, error) {
	w, err := wallet.NewWalletFromFile(cfg.Wallet)
	if err != nil {
		return nil, err
	}

	addr := w.GetChangeAddress()
	if cfg.Address != "" {
		addr, err = flags.ParseAddress(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("invalid address")
		}
	}
	acc := w.GetAccount(addr)
	err = acc.Decrypt(cfg.Password, w.Scrypt)
	if err != nil {
		return nil, err
	}

	return acc, nil
}

func newAddress(cnrID cid.ID, objID oid.ID) oid.Address {
	var addr oid.Address
	addr.SetContainer(cnrID)
	addr.SetObject(objID)
	return addr
}

func formObject(own *user.ID, cnrID cid.ID, name string, header map[string]string) *object.Object {
	attributes := make([]object.Attribute, 0, 1+len(header))
	filename := object.NewAttribute()
	filename.SetKey(object.AttributeFileName)
	filename.SetValue(name)

	attributes = append(attributes, *filename)

	for key, val := range header {
		attr := object.NewAttribute()
		attr.SetKey(key)
		attr.SetValue(val)
		attributes = append(attributes, *attr)
	}

	obj := object.New()
	obj.SetOwnerID(own)
	obj.SetContainerID(cnrID)
	obj.SetAttributes(attributes...)

	return obj
}

func newDir(cnrID cid.ID, cnr *container.Container) *fs.Dir {
	remote := cnrID.EncodeToString()
	timestamp := container.CreatedAt(*cnr)

	if domain := container.ReadDomain(*cnr); domain.Name() != "" {
		remote = domain.Name()
	}

	dir := fs.NewDir(remote, timestamp)
	dir.SetID(cnrID.String())

	return dir
}
