package neofs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	resolver "github.com/nspcc-dev/neofs-sdk-go/ns"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/walk"
	"github.com/rclone/rclone/lib/bucket"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "neofs",
		Description: "Distributed, decentralized object storage NeoFS",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name:     "neofs_endpoint",
				Help:     "Endpoints to connect to NeoFS node",
				Required: true,
				Examples: []fs.OptionExample{
					{
						Value: "s01.neofs.devenv:8080",
						Help:  "One endpoint.",
					},
					{
						Value: "s01.neofs.devenv:8080 s02.neofs.devenv:8080",
						Help:  "Multiple endpoints to form pool.",
					},
					{
						Value: "s01.neofs.devenv:8080,1 s02.neofs.devenv:8080,2",
						Help:  "Multiple endpoints with priority (less value is higher priority). Until s01 is healthy all request will be send to it.",
					},
					{
						Value: "s01.neofs.devenv:8080,1,1 s02.neofs.devenv:8080,2,1 s03.neofs.devenv:8080,2,9",
						Help:  "Multiple endpoints with priority and weights. After s01 is unhealthy requests will be send to s02 and s03 in proportions 10% and 90% respectively.",
					},
				},
			},
			{
				Name:    "neofs_connection_timeout",
				Default: 4 * time.Second,
				Help:    "NeoFS connection timeout",
			},
			{
				Name:    "neofs_request_timeout",
				Default: 4 * time.Second,
				Help:    "NeoFS request timeout",
			},
			{
				Name:    "neofs_rebalance_interval",
				Default: 15 * time.Second,
				Help:    "NeoFS rebalance connections interval",
			},
			{
				Name:    "neofs_session_expiration",
				Default: math.MaxUint32,
				Help:    "NeoFS session expiration epoch",
			},
			{
				Name: "rpc_endpoint",
				Help: "Endpoint to connect to Neo rpc node",
			},
			{
				Name:     "wallet",
				Help:     "Path to wallet",
				Required: true,
			},
			{
				Name: "address",
				Help: "Address of account",
			},
			{
				Name: "password",
				Help: "Password to decrypt wallet",
			},
			{
				Name:    "placement_policy",
				Default: "REP 3",
				Help:    "Placement policy for new containers",
				Examples: []fs.OptionExample{
					{
						Value: "REP 3",
						Help:  "Container will have 3 replicas",
					},
				},
			},
			{
				Name:    "basic_acl",
				Default: "0x1fbf8cff",
				Help:    "Basic ACL for new containers",
				Examples: []fs.OptionExample{
					{
						Value: "public-read-write",
						Help:  "Public container, anyone can read and write",
					},
					{
						Value: "0x1fffffff",
						Help:  "Public container, anyone can read and write",
					},
					{
						Value: "private",
						Help:  "Private container, only owner has access to it",
					},
				},
			},
		},
	})
}

const (
	attrFilePath = "FilePath"
)

// Options defines the configuration for this backend
type Options struct {
	NeofsEndpoint                  string      `config:"neofs_endpoint"`
	NeofsConnectionTimeout         fs.Duration `config:"neofs_connection_timeout"`
	NeofsRequestTimeout            fs.Duration `config:"neofs_request_timeout"`
	NeofsRebalanceInterval         fs.Duration `config:"neofs_rebalance_interval"`
	NeofsSessionExpirationDuration uint64      `config:"neofs_session_expiration_duration"`
	RpcEndpoint                    string      `config:"rpc_endpoint"`
	Wallet                         string      `config:"wallet"`
	Address                        string      `config:"address"`
	Password                       string      `config:"password"`
	PlacementPolicy                string      `config:"placement_policy"`
	BasicACLStr                    string      `config:"basic_acl"`
	BasicACL                       acl.Basic   `config:"-"`
}

type Fs struct {
	name          string // the name of the remote
	root          string // root of the bucket - ignore all objects above this
	opt           *Options
	ctx           context.Context
	pool          *pool.Pool
	owner         *user.ID
	rootContainer string
	rootDirectory string
	features      *fs.Features
	resolver      *resolver.NNS
}

type searchFilter struct {
	Header    string
	Value     string
	MatchType object.SearchMatchType
}

type Object struct {
	*object.Object
	fs          *Fs
	name        string
	remote      string
	filePath    string
	contentType string
	timestamp   time.Time
}

func (f *Fs) Shutdown(_ context.Context) error {
	f.pool.Close()
	return nil
}

func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(ctx context.Context, name string, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	err = opt.BasicACL.DecodeString(opt.BasicACLStr)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse basic acl: %w", err)
	}

	acc, err := getAccount(opt)
	if err != nil {
		return nil, err
	}

	p, err := createPool(ctx, acc.PrivateKey(), opt)
	if err != nil {
		return nil, err
	}

	nnsResolver, err := createNNSResolver(opt)
	if err != nil {
		return nil, err
	}

	var owner user.ID
	user.IDFromKey(&owner, acc.PrivateKey().PrivateKey.PublicKey)

	f := &Fs{
		name:     name,
		opt:      opt,
		ctx:      ctx,
		pool:     p,
		owner:    &owner,
		resolver: nnsResolver,
	}

	f.setRoot(root)

	f.features = (&fs.Features{
		DuplicateFiles:    true,
		ReadMimeType:      true,
		WriteMimeType:     true,
		BucketBased:       true,
		BucketBasedRootOK: true,
	}).Fill(ctx, f)

	if f.rootContainer != "" && f.rootDirectory != "" && !strings.HasSuffix(root, "/") {
		// Check to see if the (container,directory) is actually an existing file
		oldRoot := f.root
		newRoot, leaf := path.Split(oldRoot)
		f.setRoot(newRoot)
		_, err := f.NewObject(ctx, leaf)
		if err != nil {
			// File doesn't exist or is a directory so return old f
			f.setRoot(oldRoot)
			return f, nil
		}
		// return an error with a fs which points to the parent
		return f, fs.ErrorIsFile
	}

	return f, nil
}

func newObject(f *Fs, obj object.Object, container string) *Object {
	// we should not include rootDirectory into remote name
	prefix := f.rootDirectory
	if prefix != "" {
		prefix += "/"
	}

	objID, _ := obj.ID()

	objInfo := &Object{
		Object: &obj,
		fs:     f,
		name:   objID.EncodeToString(),
	}

	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeFileName {
			objInfo.name = attr.Value()
		}
		if attr.Key() == attrFilePath {
			objInfo.filePath = attr.Value()
		}
		if attr.Key() == object.AttributeContentType {
			objInfo.contentType = attr.Value()
		}
		if attr.Key() == object.AttributeTimestamp {
			value, err := strconv.ParseInt(attr.Value(), 10, 64)
			if err != nil {
				continue
			}
			objInfo.timestamp = time.Unix(value, 0)
		}
	}

	if objInfo.filePath == "" {
		objInfo.filePath = objInfo.name
	}

	objInfo.remote = objInfo.filePath
	if strings.Contains(objInfo.remote, prefix) {
		objInfo.remote = objInfo.remote[len(prefix):]
		if container != "" && f.rootContainer == "" {
			// we should add container name to remote name if root is empty
			objInfo.remote = path.Join(container, objInfo.remote)
		}
	}

	return objInfo
}

func (o *Object) MimeType(_ context.Context) string {
	return o.contentType
}

func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.filePath
}

func (o *Object) ID() string {
	return o.ObjectID().EncodeToString()
}

func (o *Object) ObjectID() oid.ID {
	objID, _ := o.Object.ID()
	return objID
}

func (o *Object) Remote() string {
	return o.remote
}

func (o *Object) ModTime(_ context.Context) time.Time {
	return o.timestamp
}

func (o *Object) Size() int64 {
	return int64(o.PayloadSize())
}

func (o *Object) Fs() fs.Info {
	return o.fs
}

func (o *Object) Hash(_ context.Context, ty hash.Type) (string, error) {
	if ty == hash.SHA256 {
		payloadCheckSum, _ := o.PayloadChecksum()
		return hex.EncodeToString(payloadCheckSum.Value()), nil
	}
	return "", nil
}

func (o *Object) Storable() bool {
	return true
}

func (o *Object) SetModTime(_ context.Context, _ time.Time) error {
	return fs.ErrorCantSetModTime
}

// BuffCloser is wrapper to load files from neofs.
type BuffCloser struct {
	io.Reader
}

func (bc *BuffCloser) Close() error {
	return nil
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var isRange bool
	offset, length := uint64(0), o.PayloadSize()
	fs.FixRangeOption(options, int64(o.PayloadSize()))
	for _, option := range options {
		switch option := option.(type) {
		case *fs.RangeOption:
			isRange = true
			offset = uint64(option.Start)
			if option.End < 0 {
				option.End = int64(o.PayloadSize()) + option.End
			}
			length = uint64(option.End - option.Start + 1)
		case *fs.SeekOption:
			isRange = true
			offset = uint64(option.Offset)
			length = o.PayloadSize() - uint64(option.Offset)
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}

	cnrID, _ := o.ContainerID()
	addr := newAddress(cnrID, o.ObjectID())

	if isRange {
		var prm pool.PrmObjectRange
		prm.SetAddress(addr)
		prm.SetOffset(offset)
		prm.SetLength(length)

		rangeReader, err := o.fs.pool.ObjectRange(ctx, prm)
		if err != nil {
			return nil, err
		}
		return &rangeReader, nil
	}

	var prm pool.PrmObjectGet
	prm.SetAddress(addr)

	// we cannot use ObjectRange in this case because it panics if zero length range is requested
	res, err := o.fs.pool.GetObject(ctx, prm)
	if err != nil {
		return nil, fmt.Errorf("couldn't get object %s: %w", addr, err)
	}

	return res.Payload, nil
}

func (f *Fs) Name() string {
	return f.name
}

func (f *Fs) Root() string {
	return f.root
}

func (f *Fs) String() string {
	if f.rootContainer == "" {
		return fmt.Sprintf("NeoFS root")
	}
	if f.rootDirectory == "" {
		return fmt.Sprintf("NeoFS container %s", f.rootContainer)
	}
	return fmt.Sprintf("NeoFS container %s path %s", f.rootContainer, f.rootDirectory)
}

func (f *Fs) Precision() time.Duration {
	return time.Second
}

func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.SHA256)
}

func (f *Fs) Features() *fs.Features {
	return f.features
}

func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	containerStr, containerPath := bucket.Split(path.Join(f.root, dir))

	if containerStr == "" {
		if containerPath != "" {
			return nil, fs.ErrorListBucketRequired
		}
		return f.listContainers(ctx)
	}

	return f.listEntries(ctx, containerStr, containerPath, dir, false)
}

func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	containerStr, containerPath := bucket.Split(path.Join(f.root, dir))

	list := walk.NewListRHelper(callback)

	if containerStr == "" {
		if containerPath != "" {
			return fs.ErrorListBucketRequired
		}
		containers, err := f.listContainers(ctx)
		if err != nil {
			return err
		}
		for _, containerDir := range containers {
			if err = f.listR(ctx, list, containerDir.Remote(), containerPath, dir); err != nil {
				return err
			}
		}
		return list.Flush()
	}

	if err := f.listR(ctx, list, containerStr, containerPath, dir); err != nil {
		return err
	}
	return list.Flush()
}

func (f *Fs) listR(ctx context.Context, list *walk.ListRHelper, containerStr, containerPath, dir string) error {
	entries, err := f.listEntries(ctx, containerStr, containerPath, dir, true)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = list.Add(entry); err != nil {
			return err
		}
	}

	return nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	containerStr, containerPath := bucket.Split(filepath.Join(f.root, src.Remote()))

	if mimeTyper, ok := src.(fs.MimeTyper); ok {
		options = append(options, &fs.HTTPOption{
			Key:   object.AttributeContentType,
			Value: mimeTyper.MimeType(ctx),
		})
	}

	cnrID, err := f.parseContainer(ctx, containerStr)
	if err != nil {
		return nil, fs.ErrorDirNotFound
	}

	ids, err := f.findObjectsFilePath(ctx, cnrID, containerPath)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{attrFilePath: containerPath}

	for _, option := range options {
		key, value := option.Header()
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "":
			// ignore
		case "content-type":
			headers[object.AttributeContentType] = value
		case "timestamp":
			headers[object.AttributeTimestamp] = value
		default:
			if value != "" {
				headers[key] = value
			}
		}
	}

	if headers[object.AttributeTimestamp] == "" {
		headers[object.AttributeTimestamp] = strconv.FormatInt(src.ModTime(ctx).UTC().Unix(), 10)
	}

	objHeader := formObject(f.owner, cnrID, filepath.Base(containerPath), headers)

	var prmPut pool.PrmObjectPut
	prmPut.SetHeader(*objHeader)
	prmPut.SetPayload(in)

	objID, err := f.pool.PutObject(ctx, prmPut)
	if err != nil {
		return nil, err
	}

	var prmHead pool.PrmObjectHead
	prmHead.SetAddress(newAddress(cnrID, objID))

	obj, err := f.pool.HeadObject(ctx, prmHead)
	if err != nil {
		return nil, err
	}

	for _, id := range ids {
		var prmDelete pool.PrmObjectDelete
		prmDelete.SetAddress(newAddress(cnrID, id))

		_ = f.pool.DeleteObject(ctx, prmDelete)
	}

	return newObject(f, obj, ""), nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	obj, err := o.fs.Put(ctx, in, src, options...)
	if err != nil {
		return err
	}

	objInfo, ok := obj.(*Object)
	if !ok {
		return fmt.Errorf("invalid object type")
	}

	if err = o.Remove(ctx); err != nil {
		fs.Logf(o, "couldn't remove old file after update '%s': %s", o.ObjectID(), err.Error())
	}

	o.filePath = objInfo.filePath
	o.remote = objInfo.remote
	o.name = objInfo.name
	o.timestamp = objInfo.timestamp
	o.Object = objInfo.Object

	return nil
}

func (o *Object) Remove(ctx context.Context) error {
	cnrID, _ := o.ContainerID()
	var prm pool.PrmObjectDelete
	prm.SetAddress(newAddress(cnrID, o.ObjectID()))
	return o.fs.pool.DeleteObject(ctx, prm)
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	containerStr, containerPath := bucket.Split(filepath.Join(f.root, remote))

	cnrID, err := f.parseContainer(ctx, containerStr)
	if err != nil {
		return nil, fs.ErrorDirNotFound
	}

	ids, err := f.findObjectsFilePath(ctx, cnrID, containerPath)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, fs.ErrorObjectNotFound
	}

	var prm pool.PrmObjectHead
	prm.SetAddress(newAddress(cnrID, ids[0]))

	obj, err := f.pool.HeadObject(ctx, prm)
	if err != nil {
		return nil, fmt.Errorf("head object: %w", err)
	}

	return newObject(f, obj, ""), nil
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	containerStr, _ := bucket.Split(path.Join(f.root, dir))
	if containerStr == "" {
		return nil
	}

	if _, err := f.parseContainer(ctx, containerStr); err == nil {
		return nil
	}

	var policy netmap.PlacementPolicy
	if err := policy.DecodeString(f.opt.PlacementPolicy); err != nil {
		return fmt.Errorf("parse placement policy: %w", err)
	}

	var cnr container.Container
	cnr.Init()
	cnr.SetPlacementPolicy(policy)
	cnr.SetOwner(*f.owner)
	cnr.SetBasicACL(f.opt.BasicACL)

	container.SetCreationTime(&cnr, time.Now())

	container.SetName(&cnr, containerStr)
	var domain container.Domain
	domain.SetName(containerStr)
	container.WriteDomain(&cnr, domain)

	var prm pool.PrmContainerPut
	prm.SetContainer(cnr)

	if _, err := f.pool.PutContainer(ctx, prm); err != nil {
		return fmt.Errorf("put container: %w", err)
	}

	return nil
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	containerStr, containerPath := bucket.Split(path.Join(f.root, dir))
	if containerStr == "" || containerPath != "" {
		return nil
	}

	cnrID, err := f.parseContainer(ctx, containerStr)
	if err != nil {
		return fs.ErrorDirNotFound
	}

	isEmpty, err := f.isContainerEmpty(ctx, cnrID)
	if err != nil {
		return err
	}
	if !isEmpty {
		return fs.ErrorDirectoryNotEmpty
	}

	var prm pool.PrmContainerDelete
	prm.SetContainerID(cnrID)

	if err = f.pool.DeleteContainer(ctx, prm); err != nil {
		return fmt.Errorf("couldn't delete container %s '%s': %w", cnrID, containerStr, err)
	}

	return nil
}

func (f *Fs) Purge(ctx context.Context, dir string) error {
	containerStr, containerPath := bucket.Split(path.Join(f.root, dir))

	cnrID, err := f.parseContainer(ctx, containerStr)
	if err != nil {
		return nil
	}

	if err = f.deleteByPrefix(ctx, cnrID, containerPath); err != nil {
		return fmt.Errorf("delete by prefix: %w", err)
	}

	return nil
}

// setRoot changes the root of the Fs
func (f *Fs) setRoot(root string) {
	f.root = strings.Trim(root, "/")
	f.rootContainer, f.rootDirectory = bucket.Split(f.root)
}

func (f *Fs) parseContainer(ctx context.Context, containerName string) (cid.ID, error) {
	var err error
	var cnrID cid.ID
	if err = cnrID.DecodeString(containerName); err == nil {
		return cnrID, nil
	}

	if f.resolver != nil {
		if cnrID, err = f.resolver.ResolveContainerName(containerName); err == nil {
			return cnrID, nil
		}
	} else {
		if dirEntries, err := f.listContainers(ctx); err == nil {
			for _, dirEntry := range dirEntries {
				if dirEntry.Remote() == containerName {
					if ider, ok := dirEntry.(fs.IDer); ok {
						return cnrID, cnrID.DecodeString(ider.ID())
					}
				}
			}
		}
	}

	return cid.ID{}, fmt.Errorf("couldn't resolve container '%s'", containerName)
}

func (f *Fs) listEntries(ctx context.Context, containerStr, containerPath, directory string, recursive bool) (fs.DirEntries, error) {
	cnrID, err := f.parseContainer(ctx, containerStr)
	if err != nil {
		return nil, fs.ErrorDirNotFound
	}

	ids, err := f.findObjectsPrefix(ctx, cnrID, containerPath)
	if err != nil {
		return nil, err
	}

	res := make([]fs.DirEntry, 0, len(ids))

	dirs := make(map[string]*fs.Dir)
	objs := make(map[string]*Object)

	for _, id := range ids {
		var prm pool.PrmObjectHead
		prm.SetAddress(newAddress(cnrID, id))
		obj, err := f.pool.HeadObject(ctx, prm)
		if err != nil {
			return nil, err
		}

		objInf := newObject(f, obj, containerStr)

		if !recursive {
			withoutPath := strings.TrimPrefix(objInf.filePath, containerPath)
			trimPrefixSlash := strings.TrimPrefix(withoutPath, "/")
			// emulate directories
			if index := strings.Index(trimPrefixSlash, "/"); index >= 0 {
				dir := fs.NewDir(filepath.Join(directory, trimPrefixSlash[:index]), time.Time{})
				dir.SetID(filepath.Join(containerPath, dir.Remote()))
				dirs[dir.ID()] = dir
				continue
			}
		}

		if o, ok := objs[objInf.remote]; !ok || o.timestamp.Before(objInf.timestamp) {
			objs[objInf.remote] = objInf
		}
	}

	for _, dir := range dirs {
		res = append(res, dir)
	}

	for _, obj := range objs {
		res = append(res, obj)
	}

	return res, nil
}

func (f *Fs) listContainers(ctx context.Context) (fs.DirEntries, error) {
	var prmList pool.PrmContainerList
	prmList.SetOwnerID(*f.owner)
	containers, err := f.pool.ListContainers(ctx, prmList)
	if err != nil {
		return nil, err
	}

	res := make([]fs.DirEntry, len(containers))

	for i, containerID := range containers {
		var prm pool.PrmContainerGet
		prm.SetContainerID(containerID)

		cnr, err := f.pool.GetContainer(ctx, prm)
		if err != nil {
			return nil, fmt.Errorf("couldn't get container '%s': %w", containerID, err)
		}

		res[i] = newDir(containerID, cnr)
	}

	return res, nil
}

func (f *Fs) findObjectsFilePath(ctx context.Context, cnrID cid.ID, filePath string) ([]oid.ID, error) {
	return f.findObjects(ctx, cnrID, searchFilter{
		Header:    attrFilePath,
		Value:     filePath,
		MatchType: object.MatchStringEqual,
	})
}

func (f *Fs) findObjectsPrefix(ctx context.Context, cnrID cid.ID, prefix string) ([]oid.ID, error) {
	return f.findObjects(ctx, cnrID, searchFilter{
		Header:    attrFilePath,
		Value:     prefix,
		MatchType: object.MatchCommonPrefix,
	})
}

func (f *Fs) findObjects(ctx context.Context, cnrID cid.ID, filters ...searchFilter) ([]oid.ID, error) {
	sf := object.NewSearchFilters()
	sf.AddRootFilter()

	for _, filter := range filters {
		sf.AddFilter(filter.Header, filter.Value, filter.MatchType)
	}

	return f.searchObjects(ctx, cnrID, sf)
}

func (f *Fs) deleteByPrefix(ctx context.Context, cnrID cid.ID, prefix string) error {
	filters := object.NewSearchFilters()
	filters.AddRootFilter()
	filters.AddFilter(attrFilePath, prefix, object.MatchCommonPrefix)

	var prmSearch pool.PrmObjectSearch
	prmSearch.SetContainerID(cnrID)
	prmSearch.SetFilters(filters)

	res, err := f.pool.SearchObjects(ctx, prmSearch)
	if err != nil {
		return fmt.Errorf("init searching using client: %w", err)
	}
	defer res.Close()

	var (
		inErr     error
		found     bool
		prmDelete pool.PrmObjectDelete
		addr      oid.Address
	)

	addr.SetContainer(cnrID)

	err = res.Iterate(func(id oid.ID) bool {
		found = true

		addr.SetObject(id)
		prmDelete.SetAddress(addr)

		if err = f.pool.DeleteObject(ctx, prmDelete); err != nil {
			inErr = fmt.Errorf("delete object: %w", err)
			return true
		}

		return false
	})
	if err == nil {
		err = inErr
	}
	if err != nil {
		return fmt.Errorf("iterate objects: %w", err)
	}

	if !found {
		return fs.ErrorDirNotFound
	}

	return nil
}

func (f *Fs) isContainerEmpty(ctx context.Context, cnrID cid.ID) (bool, error) {
	filters := object.NewSearchFilters()
	filters.AddRootFilter()

	var prm pool.PrmObjectSearch
	prm.SetContainerID(cnrID)
	prm.SetFilters(filters)

	res, err := f.pool.SearchObjects(ctx, prm)
	if err != nil {
		return false, fmt.Errorf("init searching using client: %w", err)
	}

	defer res.Close()

	isEmpty := true
	err = res.Iterate(func(id oid.ID) bool {
		isEmpty = false
		return true
	})
	if err != nil {
		return false, fmt.Errorf("iterate objects: %w", err)
	}

	return isEmpty, nil
}

func (f *Fs) searchObjects(ctx context.Context, cnrID cid.ID, filters object.SearchFilters) ([]oid.ID, error) {
	var prm pool.PrmObjectSearch
	prm.SetContainerID(cnrID)
	prm.SetFilters(filters)

	res, err := f.pool.SearchObjects(ctx, prm)
	if err != nil {
		return nil, fmt.Errorf("init searching using client: %w", err)
	}

	defer res.Close()

	var buf []oid.ID

	err = res.Iterate(func(id oid.ID) bool {
		buf = append(buf, id)
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("iterate objects: %w", err)
	}

	return buf, nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.ListRer     = &Fs{}
	_ fs.Purger      = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.Shutdowner  = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
)
