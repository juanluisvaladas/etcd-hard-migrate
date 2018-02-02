package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	//"os"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
)

type etcdFlags struct {
	etcdAddress *string
	certFile    *string
	keyFile     *string
	caFile      *string
}

func generateV3ClientConfig(flags *etcdFlags) (*clientv3.Config, error) {

	c := &clientv3.Config{
		Endpoints: []string{*(flags.etcdAddress)},
	}

	tlsinfo := transport.TLSInfo{
		CertFile: *(flags.certFile),
		KeyFile:  *(flags.keyFile),
		CAFile:   *(flags.caFile),
	}

	clientTLS, err := tlsinfo.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("Error while creating etcd client: %v", err)
	}
	c.TLS = clientTLS
	return c, nil
}

func wipeEtcd3(v3Config clientv3.Config) error {
	c, err := clientv3.New(v3Config)
	if err != nil {
		return err
	}
	defer c.Close()

	_, err = c.Delete(context.Background(), "/", clientv3.WithFromKey())
	if err != nil {
		return err
	}
	log.Print("Successfully wiped etcd3")
	return nil
}

func createEtcdNodeRecursive(c clientv3.Client, nodes client.Nodes) error {
	for _, node := range nodes {
		if node.Dir {
			err := createEtcdNodeRecursive(c, node.Nodes)
			if err != nil {
				return err
			}
		} else {
			_, err := c.Put(context.Background(), node.Key, node.Value)
			if err != nil {
				return err
			}
			log.Print("Successfully written key: " + node.Key)
		}
	}
	return nil
}

func writeEtcd3Data(v3Config clientv3.Config, nodes client.Nodes) error {
	c, err := clientv3.New(v3Config)
	if err != nil {
		return err
	}
	defer c.Close()
	err = createEtcdNodeRecursive(*c, nodes)
	if err != nil {
		return err
	}
	return nil
}

func generateV2ClientConfig(flags *etcdFlags) (client.Client, error) {
	tlsInfo := transport.TLSInfo{
		CertFile: *(flags.certFile),
		KeyFile:  *(flags.keyFile),
		CAFile:   *(flags.caFile),
	}

	tr, err := transport.NewTransport(tlsInfo, 30*time.Second)
	if err != nil {
		return nil, err
	}
	cfg := client.Config{
		Transport:               tr,
		Endpoints:               []string{*(flags.etcdAddress)},
		HeaderTimeoutPerRequest: 30 * time.Second,
	}

	return client.New(cfg)
}

func getV2Keys(c client.Client) (client.Nodes, error) {
	kapiV2 := client.NewKeysAPI(c)
	resp, err := kapiV2.Get(context.Background(), "/", &client.GetOptions{Sort: true, Recursive: true, Quorum: true})
	if err != nil {
		return nil, err
	}

	return resp.Node.Nodes, nil
}

func main() {
	flags := &etcdFlags{
		etcdAddress: flag.String("etcd-address", "", "Etcd address"),
		certFile:    flag.String("cert", "", "identify secure client using this TLS certificate file"),
		keyFile:     flag.String("key", "", "identify secure client using this TLS key file"),
		caFile:      flag.String("cacert", "", "verify certificates of TLS-enabled secure servers using this CA bundle"),
	}

	flag.Parse()
	if *(flags.etcdAddress) == "" || *(flags.certFile) == "" || *(flags.keyFile) == "" || *(flags.caFile) == "" {
		log.Fatal("--etcd-address --cert --key and --cacert are required")
	}

	v2Client, err := generateV2ClientConfig(flags)
	if err != nil {
		log.Fatal(err)
	}

	v3ClientConfig, err := generateV3ClientConfig(flags)
	if err != nil {
		log.Fatal(err)
	}
	// Already initialized
	wipeEtcd3(*v3ClientConfig)

	// fetch v2 Keys
	nodes, err := getV2Keys(v2Client)
	if err != nil {
		log.Fatal(err)
	}
	// create them in v3
	writeEtcd3Data(*v3ClientConfig, nodes)
	if err != nil {
		log.Fatal(err)
	}

}
