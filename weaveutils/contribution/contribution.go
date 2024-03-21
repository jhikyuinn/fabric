package contribution

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	structures "github.com/hyperledger/fabric/weaveutils/structures"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/lovoo/goka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type contributionCodec struct{}

type FabricChannelState struct {
	FabricChannels []FabricChannel
}

type FabricChannel struct {
	Type         int               `bson:"type" json:"type"`
	ChannelId    string            `bson:"channel" json:"channel"`
	BlockNumber  int               `bson:"block" json:"block"`
	Transactions int               `bson:"tx" json:"tx"`
	Members      int               `bson:"members" json:"members"`
	FabricPeers  []PeerAttribution `bson:"attr" json:"attr"`
}

type PeerAttribution struct {
	Id   string
	Sent int
}

var (
	topic goka.Stream = "contribution-topic"
	group goka.Group  = "contribution-group"

	tmc *goka.TopicManagerConfig

	// producerSize indicates the number of producers
	producerSize int = 20
	// defaultPartitionChannelSize is same with producerSize because producers push data into its partition
	defaultPartitionChannelSize int = 20

	mutex sync.Mutex
)

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 3
	tmc.Stream.Replication = 3
}

// Encodes FabricChannel into []byte
func (cc *contributionCodec) Encode(value interface{}) ([]byte, error) {
	if _, isState := value.(*FabricChannel); !isState {
		return nil, fmt.Errorf("codec requires value FabricChannel, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a FabricChannel from []byte to it's go representation
func (cc *contributionCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   FabricChannel
		err error
	)
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling FabricChannel: %v", err)
	}
	return &c, nil
}

func (fc *FabricChannel) getContribution() FabricChannel {
	return *fc
}

func (fc *FabricChannel) Start() {
	go fc.consumer()
}

func (fc *FabricChannel) Record(ctx goka.Context, msg interface{}) {
	mutex.Lock()
	defer mutex.Unlock()

	MongoURL := "mongodb://localhost:27017"
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(MongoURL))
	if err != nil {
		fmt.Println(err)
	}
	defer client.Disconnect(context.TODO())

	collection := client.Database("fabric").Collection("contribution")

	var recvInfo FabricChannel

	isChannelExist, r := fc.IsChannelExist(context.TODO(), ctx.Key(), collection)
	if isChannelExist {
		// LOGIC: finding peer
		// if the channel is already exist, find that the peer was joined
		for _, j := range r["FabricPeers"].(primitive.A) {
			if strings.Contains(j.(primitive.M)["id"].(string), msg.(*structures.BlockData).Id) {
				s := int(j.(primitive.M)["sent"].(int32)) + 1

				newPeer := PeerAttribution{
					Id:   msg.(*structures.BlockData).Id,
					Sent: s,
				}
				recvInfo = FabricChannel{
					Type:        3,
					ChannelId:   ctx.Key(),
					FabricPeers: []PeerAttribution{newPeer},
				}

				fc.updateChannel(context.TODO(), recvInfo, collection, r)
				return
			}
		}

		newPeer := PeerAttribution{
			Id:   msg.(*structures.BlockData).Id,
			Sent: 1,
		}
		recvInfo = FabricChannel{
			Type:        3,
			ChannelId:   ctx.Key(),
			FabricPeers: []PeerAttribution{newPeer},
		}
		fc.insertChannel(context.TODO(), recvInfo, collection)
		return
	} else {
		// if the channel is not exist, append the peer information
		newPeer := PeerAttribution{
			Id:   msg.(*structures.BlockData).Id,
			Sent: 1,
		}
		recvInfo = FabricChannel{
			Type:        3,
			ChannelId:   ctx.Key(),
			FabricPeers: []PeerAttribution{newPeer},
		}

		fc.insertChannel(context.TODO(), recvInfo, collection)
	}
}

func (fc *FabricChannel) consumer() {
	brokers := []string{os.Getenv("KAFKA_BROKER1"), os.Getenv("KAFKA_BROKER2"), os.Getenv("KAFKA_BROKER3")}
	contConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "contributionGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	contConsumer.SubscribeTopics([]string{"contribution-topic"}, nil)
	defer contConsumer.Close()

	for {
		msg, err := contConsumer.ReadMessage(-1)
		if err == nil {
			recvInfo := FabricChannel{}
			json.Unmarshal(msg.Value, &recvInfo)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			MongoURL := "mongodb://localhost:27017"
			client, err := mongo.Connect(ctx, options.Client().ApplyURI(MongoURL))
			if err != nil {
				fmt.Println(err)
			}

			collection := client.Database("fabric").Collection("contribution")
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if recvInfo.Type == 1 {
				fc.BlockNumber = recvInfo.BlockNumber
				fc.Transactions += recvInfo.Transactions
			} else if recvInfo.Type == 2 {
				fc.Members = recvInfo.Members
			}

			// LOGIC: if the channel status already exists, update the channel info
			//        else, generate new channel info and insert to the private DB
			isChannelExist, r := fc.IsChannelExist(ctx, recvInfo.ChannelId, collection)
			if isChannelExist {
				fc.updateChannel(ctx, recvInfo, collection, r)
			} else {
				fc.insertChannel(ctx, recvInfo, collection)
				continue
			}
		}
	}
}

func (fc *FabricChannel) IsChannelExist(ctx context.Context, channelId string, collection *mongo.Collection) (bool, bson.M) {
	opts := options.FindOne().SetSort(bson.D{{"ChannelId", 1}})
	var r bson.M
	err := collection.FindOne(context.TODO(), bson.D{{"ChannelId", channelId}}, opts).Decode(&r)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		} else {
			fmt.Println(err)
		}
		return false, nil
	}

	return true, r
}

func (fc *FabricChannel) insertChannel(ctx context.Context, recvInfo FabricChannel, collection *mongo.Collection) {
	res, err := collection.InsertOne(ctx, bson.D{
		{"ChannelId", recvInfo.ChannelId},
		{"BlockNumber", recvInfo.BlockNumber},
		{"Transactions", recvInfo.Transactions},
		{"Members", recvInfo.Members},
		{"FabricPeers", recvInfo.FabricPeers},
	})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully inserted to private DB", res.InsertedID)
}

func (fc *FabricChannel) updateChannel(ctx context.Context, recvInfo FabricChannel, collection *mongo.Collection, r bson.M) {
	f := collection.FindOne(context.TODO(), bson.M{"ChannelId": recvInfo.ChannelId})
	var bsonObj bson.M
	if err := f.Decode(&bsonObj); err != nil {
		fmt.Println(err)
		return
	}

	var update bson.M
	filter := bson.M{"ChannelId": recvInfo.ChannelId}
	if recvInfo.Type == 1 {
		update = bson.M{
			"$set": bson.M{
				"BlockNumber":  recvInfo.BlockNumber,
				"Transactions": int(bsonObj["Transactions"].(int32)) + recvInfo.Transactions,
			},
		}
	} else if recvInfo.Type == 2 {
		update = bson.M{
			"$set": bson.M{
				"Members": recvInfo.Members,
			},
		}
	} else if recvInfo.Type == 3 {
		update = bson.M{
			"$set": bson.M{
				"FabricPeers": recvInfo.FabricPeers,
			},
		}
	}

	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		fmt.Println(err)
	}
}
