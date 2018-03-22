package server

import (
	"fmt"
	"io"
	"net"
	"santana/commitlog"
	"santana/protocol"
	"time"
)

type KafkaServer struct {
	listener   *net.TCPListener
	LogManager *commitlog.LogManager
}

func NewKafkaServer(logManager *commitlog.LogManager) *KafkaServer {
	return &KafkaServer{
		LogManager: logManager,
	}
}

func (s *KafkaServer) Listen(addr string) error {
	protocolAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}

	s.listener, err = net.ListenTCP("tcp", protocolAddr)
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				continue
			}

			go s.handleConnection(conn)
		}
	}()

	return nil
}

func (s *KafkaServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	header := new(protocol.RequestHeader)
	p := make([]byte, 4)

	for {
		_, err := io.ReadFull(conn, p)
		if err != nil {
			break
		}

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			break
		}

		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		if _, err = io.ReadFull(conn, b[4:]); err != nil {
			panic(err)
		}

		d := protocol.NewDecoder(b)
		if err := header.Decode(d); err != nil {
			panic(err)
		}

		fmt.Printf("Received: API Key = %v, Version = %v, Id = %v\n", header.APIKey, header.APIVersion, header.CorrelationID)

		var req protocol.Decoder
		switch header.APIKey {
		case protocol.APIVersionsKey:
			req = &protocol.APIVersionsRequest{}
		case protocol.MetadataKey:
			req = &protocol.MetadataRequest{}
		case protocol.ProduceKey:
			req = &protocol.ProduceRequest{}
		case protocol.FetchKey:
			req = &protocol.FetchRequest{}
		case protocol.OffsetsKey:
			req = &protocol.OffsetsRequest{}
		default:
			panic(fmt.Errorf("no such handler exists for %v", header.APIKey))
		}

		if err := req.Decode(d); err != nil {
			panic(err)
		}

		var resp protocol.ResponseBody

		switch req.(type) {
		case *protocol.APIVersionsRequest:
			resp = &protocol.APIVersionsResponse{
				APIVersions: []protocol.APIVersion{
					{APIKey: protocol.ProduceKey, MinVersion: 2, MaxVersion: 2},
					{APIKey: protocol.FetchKey, MinVersion: 1, MaxVersion: 1},
					{APIKey: protocol.OffsetsKey, MinVersion: 0, MaxVersion: 0},
					{APIKey: protocol.MetadataKey},
					{APIKey: protocol.APIVersionsKey},
				},
			}

		case *protocol.OffsetsRequest:
			prodReq := req.(*protocol.OffsetsRequest)
			prodResp := make([]*protocol.OffsetResponse, 0, len(prodReq.Topics))

			for _, t := range prodReq.Topics {

				log, err := s.LogManager.GetLog(t.Topic)
				if err != nil {
					panic(err)
				}

				minOff := log.MinOffset()
				maxOff := log.MaxOffset()
				fmt.Printf("Offsets for %v = %v/%v\n", t.Topic, minOff, maxOff)

				partResp := make([]*protocol.PartitionResponse, 0, len(t.Partitions))
				for _, p := range t.Partitions {
					partResp = append(partResp, &protocol.PartitionResponse{
						ErrorCode: 0,
						Offsets:   []int64{maxOff},
						Partition: p.Partition,
					})
				}

				prodResp = append(prodResp, &protocol.OffsetResponse{
					PartitionResponses: partResp,
					Topic:              t.Topic,
				})
			}

			resp = &protocol.OffsetsResponse{
				Responses: prodResp,
			}

		case *protocol.MetadataRequest:
			fmt.Printf("Topics: %v\n", req.(*protocol.MetadataRequest).Topics)
			logs := s.LogManager.GetLogs()
			topicMetadata := make([]*protocol.TopicMetadata, 0)

			for _, r := range logs {
				topicMetadata = append(topicMetadata, &protocol.TopicMetadata{
					Topic:          r.Name,
					TopicErrorCode: 0,
					PartitionMetadata: []*protocol.PartitionMetadata{
						&protocol.PartitionMetadata{
							ParititionID: 0,
							Leader:       1,
						},
					},
				})
			}

			resp = &protocol.MetadataResponse{
				Brokers: []*protocol.Broker{
					&protocol.Broker{
						Host:   "localhost",
						NodeID: 1,
						Port:   9092,
					},
				},
				TopicMetadata: topicMetadata,
			}
		case *protocol.ProduceRequest:
			fmt.Printf("API version %v\n", header.APIVersion)
			prodReq := req.(*protocol.ProduceRequest)
			prodResp := make([]*protocol.ProduceResponse, 0, len(prodReq.TopicData))

			for _, t := range prodReq.TopicData {
				l, err := s.LogManager.GetLog(t.Topic)
				ppr := make([]*protocol.ProducePartitionResponse, 0, len(t.Data))

				if err != nil {
					panic(err)
				}

				fmt.Printf("Appending %v entries.\n", len(t.Data))

				for _, d := range t.Data {
					off, err := l.Append(d.RecordSet)
					if err != nil {
						panic(err)
					}

					ppr = append(ppr, &protocol.ProducePartitionResponse{
						Partition:  d.Partition,
						BaseOffset: off,
						ErrorCode:  0,
						Timestamp:  time.Now().Unix(),
					})
				}

				prodResp = append(prodResp, &protocol.ProduceResponse{
					Topic:              t.Topic,
					PartitionResponses: ppr,
				})
			}

			resp = &protocol.ProduceResponses{
				ThrottleTimeMs: 0,
				Responses:      prodResp,
			}

		case *protocol.FetchRequest:
			freq := req.(*protocol.FetchRequest)
			fres := make([]*protocol.FetchResponse, 0, len(freq.Topics))

			for _, t := range freq.Topics {
				l, err := s.LogManager.GetLog(t.Topic)
				if err != nil {
					panic(err)
				}

				pres := make([]*protocol.FetchPartitionResponse, 0, len(t.Partitions))

				for _, p := range t.Partitions {
					maxOff := l.MaxOffset()
					fmt.Printf("Requesting at %v - max %v\n", p.FetchOffset, maxOff)

					buff := make([]byte, p.MaxBytes)
					tries := 0

				try_again:
					n, err := l.ReadAt(buff, p.FetchOffset)

					if err != nil {
						if err == commitlog.ErrOffsetOutOfRange || err == io.EOF {
							if n == 0 && tries == 0 {
								tries++
								time.Sleep(time.Duration(freq.MaxWaitTime) * time.Millisecond)
								goto try_again
							}
						} else {
							fmt.Printf("Fetch Response: %s\n", err.Error())

							pres = append(pres, &protocol.FetchPartitionResponse{
								ErrorCode:     -1,
								Partition:     p.Partition,
								RecordSet:     []byte{},
								HighWatermark: l.MaxOffset(),
							})

							continue
						}
					}

					pres = append(pres, &protocol.FetchPartitionResponse{
						ErrorCode:     0,
						Partition:     p.Partition,
						RecordSet:     buff[:n],
						HighWatermark: l.MaxOffset(),
					})
				}

				fres = append(fres, &protocol.FetchResponse{
					PartitionResponses: pres,
					Topic:              t.Topic,
				})
			}

			resp = &protocol.FetchResponses{
				Responses:      fres,
				ThrottleTimeMs: 0,
			}
		}

		if resp == nil {
			continue
		}

		var response protocol.Encoder
		response = &protocol.Response{
			Body:          resp,
			CorrelationID: header.CorrelationID,
		}

		en, err := protocol.Encode(response)
		if err != nil {
			panic(err)
		}

		_, err = conn.Write(en)
		if err != nil {
			panic(err)
		}
	}
}
