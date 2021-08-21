package metastore

import (
	"context"
	"github.com/akozlenkov/metastore/thrift/gen-go/hive_metastore"
	"github.com/apache/thrift/lib/go/thrift"
	"net/url"
)

type ClientConfig struct {
	Url  string
	Sasl bool
}

type Client struct {
	config    ClientConfig
	context   context.Context
	transport thrift.TTransport
	client    *hive_metastore.ThriftHiveMetastoreClient
}

func NewMetastoreClient(config ClientConfig) (*Client, error) {
	u, err := url.ParseRequestURI(config.Url)
	if err != nil {
		return nil, err
	}

	socket, err := thrift.NewTSocket(u.Host)
	if err != nil {
		return nil, err
	}

	var transport thrift.TTransport

	if config.Sasl {
		if transport, err = NewTSaslTransport(socket, u.Hostname(), "GSSAPI", map[string]string{"service": "hive"}); err != nil {
			return nil, err
		}
	} else {
		if transport, err = thrift.NewTBufferedTransportFactory(24 * 1024 * 1024).GetTransport(socket); err != nil {
			return nil, err
		}
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	client := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(protocolFactory.GetProtocol(transport), protocolFactory.GetProtocol(transport)))
	if err := transport.Open(); err != nil {
		return nil, err
	}

	return &Client{config, context.Background(), transport, client}, nil

}

func (c *Client) Clone() (*Client, error) {
	return NewMetastoreClient(c.config)
}

func (c *Client) Close() (err error) {
	return c.transport.Close()
}

func (c *Client) GetAllDatabases() ([]string, error) {
	return c.client.GetAllDatabases(c.context)
}

func (c *Client) GetDatabases(pattern string) ([]string, error) {
	return c.client.GetDatabases(c.context, pattern)
}

func (c *Client) GetDatabase(name string) (*hive_metastore.Database, error) {
	return c.client.GetDatabase(c.context, name)
}

func (c *Client) CreateDatabase(database *hive_metastore.Database) error {
	return c.client.CreateDatabase(c.context, database)
}

func (c *Client) AlterDatabase(dbname string, db *hive_metastore.Database) error {
	return c.client.AlterDatabase(c.context, dbname, db)
}

func (c *Client) DropDatabase(name string, deleteData, cascade bool) error {
	return c.client.DropDatabase(c.context, name, deleteData, cascade)
}

func (c *Client) GetAllTables(dbname string) ([]string, error) {
	return c.client.GetAllTables(c.context, dbname)
}

func (c *Client) GetTables(dbname string, pattern string) ([]string, error) {
	return c.client.GetTables(c.context, dbname, pattern)
}

func (c *Client) GetTable(dbname, tblname string) (*hive_metastore.Table, error) {
	return c.client.GetTable(c.context, dbname, tblname)
}

func (c *Client) GetTableMeta(dbname, tblPattern string, tableTypes []string) ([]*hive_metastore.TableMeta, error) {
	return c.client.GetTableMeta(c.context, dbname, tblPattern, tableTypes)
}

func (c *Client) CreateTable(table *hive_metastore.Table) error {
	return c.client.CreateTable(c.context, table)
}

func (c *Client) AlterTable(dbname, tblname string, table *hive_metastore.Table) error {
	return c.client.AlterTable(c.context, dbname, tblname, table)
}

func (c *Client) DropTable(dbname, tblname string, deleteData bool) error {
	return c.client.DropTable(c.context, dbname, tblname, deleteData)
}
