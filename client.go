package metastore

import (
	"context"
	"errors"
	"fmt"
	"github.com/akozlenkov/metastore/thrift/gen-go/hive_metastore"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/hlts2/round-robin"
	"net/url"
	"strings"
)

type Client struct {
	errs       int
	uris       []*url.URL
	security   map[string]string
	roundRobin roundrobin.RoundRobin
}

type Connection struct {
	context   context.Context
	client    *hive_metastore.ThriftHiveMetastoreClient
	transport thrift.TTransport
}

func NewClient(dsn string, security map[string]string) (*Client, error) {
	uris := make([]*url.URL, 0)
	for _, u := range strings.Split(dsn, ",") {
		uri, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		uris = append(uris, uri)
	}
	roundRobin, err := roundrobin.New(uris)
	if err != nil {
		return nil, err
	}

	return &Client{errs: 0, uris: uris, security: security, roundRobin: roundRobin}, nil
}

func (c *Client) Connect() (*Connection, error) {
	if c.errs > len(c.uris) {
		return nil, errors.New("servers are unavailable")
	}

	var transport thrift.TTransport

	uri := c.roundRobin.Next()

	socket, err := thrift.NewTSocket(uri.Host)
	if err != nil {
		return nil, err
	}

	if c.security != nil {
		if _, ok := c.security["auth_mechanisms"]; !ok {
			return nil, fmt.Errorf("auth_mechanisms must be set in config")
		}
		transport, err = NewTSaslTransport(socket, uri.Hostname(), c.security["auth_mechanisms"], c.security)
		if err != nil {
			return nil, err
		}
	} else {
		transport, err = thrift.NewTBufferedTransportFactory(24 * 1024 * 1024).GetTransport(socket)
		if err != nil {
			return nil, err
		}
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	client := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(protocolFactory.GetProtocol(transport), protocolFactory.GetProtocol(transport)))
	if err := transport.Open(); err != nil {
		c.errs++
		return c.Connect()
	}

	c.errs = 0

	return &Connection{
		client:    client,
		transport: transport,
		context:   context.Background(),
	}, nil
}

func (c *Connection) Close() (err error) {
	return c.transport.Close()
}

func (c *Connection) GetAllDatabases() ([]string, error) {
	return c.client.GetAllDatabases(c.context)
}

func (c *Connection) GetDatabases(pattern string) ([]string, error) {
	return c.client.GetDatabases(c.context, pattern)
}

func (c *Connection) GetDatabase(name string) (*hive_metastore.Database, error) {
	return c.client.GetDatabase(c.context, name)
}

func (c *Connection) CreateDatabase(database *hive_metastore.Database) error {
	return c.client.CreateDatabase(c.context, database)
}

func (c *Connection) AlterDatabase(dbname string, db *hive_metastore.Database) error {
	return c.client.AlterDatabase(c.context, dbname, db)
}

func (c *Connection) DropDatabase(name string, deleteData, cascade bool) error {
	return c.client.DropDatabase(c.context, name, deleteData, cascade)
}

func (c *Connection) GetAllTables(dbname string) ([]string, error) {
	return c.client.GetAllTables(c.context, dbname)
}

func (c *Connection) GetTables(dbname, pattern string) ([]string, error) {
	return c.client.GetTables(c.context, dbname, pattern)
}

func (c *Connection) GetTable(dbname, tblname string) (*hive_metastore.Table, error) {
	return c.client.GetTable(c.context, dbname, tblname)
}

func (c *Connection) CreateTable(table *hive_metastore.Table) error {
	return c.client.CreateTable(c.context, table)
}

func (c *Connection) AlterTable(dbname, tblname string, table *hive_metastore.Table) error {
	return c.client.AlterTable(c.context, dbname, tblname, table)
}

func (c *Connection) DropTable(dbname, tblname string, deleteData bool) error {
	return c.client.DropTable(c.context, dbname, tblname, deleteData)
}

func (c *Connection) GetPartitions(dbname, tblname string, limit int16) ([]string, error) {
	return c.client.GetPartitionNames(c.context, dbname, tblname, limit)
}

func (c *Connection) GetRoles() ([]string, error) {
	return c.client.GetRoleNames(c.context)
}

func (c *Connection) CreateRole(name, owner string) (bool, error) {
	return c.client.CreateRole(c.context, &hive_metastore.Role{RoleName: name, OwnerName: owner})
}

func (c *Connection) ListRoles(pname string, ptype hive_metastore.PrincipalType) ([]*hive_metastore.Role, error) {
	return c.client.ListRoles(c.context, pname, ptype)
}

func (c *Connection) DropRole(name string) (bool, error) {
	return c.client.DropRole(c.context, name)
}

func (c *Connection) ListPrivileges(pname string, ptype hive_metastore.PrincipalType, ref *hive_metastore.HiveObjectRef) ([]*hive_metastore.HiveObjectPrivilege, error) {
	return c.client.ListPrivileges(c.context, pname, ptype, ref)
}

func (c *Connection) GetPrivilegeSet(ref *hive_metastore.HiveObjectRef, user string, groups []string) (*hive_metastore.PrincipalPrivilegeSet, error) {
	return c.client.GetPrivilegeSet(c.context, ref, user, groups)
}

func (c *Connection) GetPrincipalsInRole(name string) ([]*hive_metastore.RolePrincipalGrant, error) {
	r, err := c.client.GetPrincipalsInRole(c.context, &hive_metastore.GetPrincipalsInRoleRequest{RoleName: name})
	if err != nil {
		return nil, err
	}
	return r.GetPrincipalGrants(), nil
}

func (c *Connection) GetRoleGrantsForPrincipal(pname string, ptype hive_metastore.PrincipalType) ([]*hive_metastore.RolePrincipalGrant, error) {
	r, err := c.client.GetRoleGrantsForPrincipal(c.context, &hive_metastore.GetRoleGrantsForPrincipalRequest{
		PrincipalName: pname,
		PrincipalType: ptype,
	})
	if err != nil {
		return nil, err
	}
	return r.GetPrincipalGrants(), nil
}

func (c *Connection) GrantRole(role, pname string, ptype hive_metastore.PrincipalType, gname string, gtype hive_metastore.PrincipalType) (bool, error) {
	return c.client.GrantRole(c.context, role, pname, ptype, gname, gtype, true)
}
