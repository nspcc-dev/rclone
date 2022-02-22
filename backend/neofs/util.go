package neofs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	rpc "github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	cntnr "github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/object/address"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/resolver"
	"github.com/rclone/rclone/fs"
)

func createPool(ctx context.Context, key *keys.PrivateKey, cfg *Options) (pool.Pool, error) {
	pb := new(pool.Builder)
	pb.AddNode(cfg.NeofsEndpoint, 1, 1)

	opts := &pool.BuilderOptions{
		Key:                       &key.PrivateKey,
		NodeConnectionTimeout:     time.Duration(cfg.NeofsConnectionTimeout),
		NodeRequestTimeout:        time.Duration(cfg.NeofsRequestTimeout),
		ClientRebalanceInterval:   time.Duration(cfg.NeofsRebalanceInterval),
		SessionExpirationDuration: cfg.NeofsSessionExpirationDuration,
	}

	return pb.Build(ctx, opts)
}

func createNnsResolver(ctx context.Context, cfg *Options) (resolver.NNSResolver, error) {
	if cfg.RpcEndpoint == "" {
		return nil, nil
	}
	cli, err := rpc.New(ctx, cfg.RpcEndpoint, rpc.Options{})
	if err != nil {
		return nil, err
	}
	if err = cli.Init(); err != nil {
		return nil, err
	}

	return resolver.NewNNSResolver(cli)
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

func newAddress(cnrID *cid.ID, id *oid.ID) *address.Address {
	addr := address.NewAddress()
	addr.SetContainerID(cnrID)
	addr.SetObjectID(id)
	return addr
}

func formRawObject(own *owner.ID, cnrID *cid.ID, name string, header map[string]string) *object.RawObject {
	attributes := make([]*object.Attribute, 0, 1+len(header))
	filename := object.NewAttribute()
	filename.SetKey(object.AttributeFileName)
	filename.SetValue(name)

	attributes = append(attributes, filename)

	for key, val := range header {
		attr := object.NewAttribute()
		attr.SetKey(key)
		attr.SetValue(val)
		attributes = append(attributes, attr)
	}

	raw := object.NewRaw()
	raw.SetOwnerID(own)
	raw.SetContainerID(cnrID)
	raw.SetAttributes(attributes...)

	return raw
}

func newDir(cnrID *cid.ID, cnr *container.Container) *fs.Dir {
	remote := cnrID.String()
	timestamp := time.Time{}
	for _, attr := range cnr.Attributes() {
		if attr.Key() == container.AttributeTimestamp {
			value, err := strconv.ParseInt(attr.Value(), 10, 64)
			if err != nil {
				fs.Logf("couldn't parse timestamp '%s': %s", attr.Value(), err.Error())
				continue
			}
			timestamp = time.Unix(value, 0)
		}
		if attr.Key() == cntnr.SysAttributeName {
			remote = attr.Value()
		}
	}

	dir := fs.NewDir(remote, timestamp)
	dir.SetID(cnrID.String())

	return dir
}
