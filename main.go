package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/pkg/transport"
)

type etcdFlags struct {
	etcdAddress *string
	certFile    *string
	keyFile     *string
	caFile      *string
}

func createEtcdNodeRecursive(k client.KeysAPI, nodes client.Nodes) error {
	for _, node := range nodes {
		if node.Dir {
			log.Println("Iterating over key: " + node.Key)
			_, err := k.Set(context.Background(), node.Key, "", &client.SetOptions{TTL: node.TTLDuration(), Dir: true})
			if err != nil {
				return err
			}
			err = createEtcdNodeRecursive(k, node.Nodes)
			if err != nil {
				return err
			}
		} else {

			_, err := k.Create(context.Background(), node.Key, node.Value)
			if err != nil {
				return err
			}
			log.Println("Successfully written key: " + node.Key)
		}
	}
	return nil
}

func writeEtcdData(k client.KeysAPI, nodes client.Nodes) error {
	err := createEtcdNodeRecursive(k, nodes)
	if err != nil {
		return err
	}
	return nil
}

func generateV2Client(flags *etcdFlags) (client.KeysAPI, error) {

	cfg := client.Config{
		Endpoints:               []string{*(flags.etcdAddress)},
		HeaderTimeoutPerRequest: 30 * time.Second,
	}

	if *(flags.certFile) != "" || *(flags.keyFile) != "" || *(flags.caFile) != "" {
		tlsInfo := transport.TLSInfo{
			CertFile: *(flags.certFile),
			KeyFile:  *(flags.keyFile),
			CAFile:   *(flags.caFile),
		}

		tr, err := transport.NewTransport(tlsInfo, 30*time.Second)
		if err != nil {
			return nil, err
		}
		cfg.Transport = tr
	}

	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}

	return client.NewKeysAPI(c), nil

}

func getV2Keys(k client.KeysAPI) (client.Nodes, error) {
	resp, err := k.Get(context.Background(), "/", &client.GetOptions{Sort: true, Recursive: true, Quorum: true})
	if err != nil {
		return nil, err
	}

	return resp.Node.Nodes, nil
}

func main() {
	destFlags := &etcdFlags{
		etcdAddress: flag.String("dest-etcd-address", "", "Etcd address"),
		certFile:    flag.String("dest-cert", "", "identify secure client using this TLS certificate file"),
		keyFile:     flag.String("dest-key", "", "identify secure client using this TLS key file"),
		caFile:      flag.String("dest-cacert", "", "verify certificates of TLS-enabled secure servers using this CA bundle"),
	}

	srcFlags := &etcdFlags{
		etcdAddress: flag.String("src-etcd-address", "", "Etcd address"),
		certFile:    flag.String("src-cert", "", "identify secure client using this TLS certificate file"),
		keyFile:     flag.String("src-key", "", "identify secure client using this TLS key file"),
		caFile:      flag.String("src-cacert", "", "verify certificates of TLS-enabled secure servers using this CA bundle"),
	}

	flag.Parse()
	if *(srcFlags.etcdAddress) == "" || *(srcFlags.certFile) == "" || *(srcFlags.keyFile) == "" || *(srcFlags.caFile) == "" {
		log.Fatal("--src-etcd-address --src-cert --src-key and --cacert are required")
	}
	if *(destFlags.etcdAddress) == "" {
		log.Fatal("--dest-etcd-address is required")
	}

	srcClient, err := generateV2Client(srcFlags)
	if err != nil {
		log.Fatal(err)
	}

	destClient, err := generateV2Client(destFlags)
	if err != nil {
		log.Fatal(err)
	}

	// fetch v2 Keys
	nodes, err := getV2Keys(srcClient)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(nodes)
	err = writeEtcdData(destClient, nodes)
	if err != nil {
		log.Fatal(err)
	}

}
