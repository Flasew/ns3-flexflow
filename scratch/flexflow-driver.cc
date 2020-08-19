#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/network-module.h"
#include "ns3/packet-sink.h"
#include "ns3/csma-module.h"
#include "ns3/core-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/traffic-control-module.h"
#include "ns3/flow-monitor-module.h"

#include "json.hpp"

#include <unordered_map>
#include <fstream>
#include <streambuf>
#include <iostream>
#include <random>

using namespace ns3;
using nlohmann::json;

NS_LOG_COMPONENT_DEFINE("FlexFlowDriver");

class FlexFlowDriver {

public:
  FlexFlowDriver(uint32_t numNode);
  FlexFlowDriver(uint32_t numNode, uint64_t bandwidth, Time latency);
  FlexFlowDriver(uint32_t numNode, uint64_t bandwidth, Time latency, int maxLink, int maxIf);

  // std::vector<std::vector<uint32_t> > GenerateRandomConnection();
  // std::vector<std::vector<uint32_t> > ModifyConnectionRandom(int nToModify = 1);
  
  void SetBandwidth(uint64_t bw) {bandwidth = bw;}
  void SetLatency(Time lat) {latency = lat;}
  void SetFullMeshTopology();
  void SetUpNetwork();
  void LoadFFTaskGraph(const std::string & taskgraph);

private:
  NodeContainer allNodes;

  std::vector<std::vector<uint32_t> > connectionMatrix;

  std::vector<Ptr<FlexFlowApplication> > ffapps;
  std::vector<std::vector<Ptr<NetDevice> > > netdevices;
  std::vector<std::vector<Ipv4Address> > interfaceAddrs;

  int maxAllowedLink;
  int interfaceCnt;
  uint32_t numNode;

  uint64_t bandwidth;
  Time latency;

  NodeContainer GetContainerOf(std::initializer_list<uint32_t> args) {
    NodeContainer r;
    for (auto i: args) {
      r.Add(allNodes.Get(i));
    }
    return r;
  }

};

FlexFlowDriver::FlexFlowDriver(uint32_t numNode) : numNode(numNode) {
  allNodes.Create(numNode);

  InternetStackHelper internet;
  internet.Install (allNodes);

  connectionMatrix.resize(numNode, std::vector<uint32_t>(numNode));
  netdevices.resize(numNode, std::vector<Ptr<NetDevice> >(numNode));
  interfaceAddrs.resize(numNode, std::vector<Ipv4Address>(numNode));
  ffapps.resize(numNode);
}

FlexFlowDriver::FlexFlowDriver(uint32_t numNode, uint64_t bandwidth, Time latency, int maxLink, int maxIf)
 : FlexFlowDriver(numNode) { 
  this->bandwidth = bandwidth;
  this->latency = latency; 
  this->maxAllowedLink = maxLink;
  this->interfaceCnt = maxIf;
}

FlexFlowDriver::FlexFlowDriver(uint32_t numNode, uint64_t bandwidth, Time latency) 
 : FlexFlowDriver(numNode) {
  this->bandwidth = bandwidth;
  this->latency = latency; 
}

void FlexFlowDriver::SetFullMeshTopology() {
  maxAllowedLink = numNode * (numNode - 1) / 2;
  interfaceCnt = numNode - 1;
  for (int i = 0; i < numNode; i++) {
    for (int j = 0; j < numNode; j++) {
      if (i != j) {
        connectionMatrix[i][j] = 1;
      } else {
        connectionMatrix[i][j] = 0;
      }
    }
  }
}

// assumes the connection matrix and link parameters has been specified
void FlexFlowDriver::SetUpNetwork() {
  for (int i = 0; i < numNode; i++) {
    ffapps[i] = CreateObject<FlexFlowApplication>();
    allNodes.Get(i)->AddApplication(ffapps[i]);
  }

  uint32_t baseAddr = Ipv4Address("10.0.0.0").Get();
  for (unsigned i = 0; i < numNode; i++) {
    for (unsigned j = 0; j < i; j++) {
      int bwMultiplier;
      if ((bwMultiplier = connectionMatrix[i][j]) > 0) {
        // add link
        PointToPointHelper p2p;
        p2p.SetDeviceAttribute("DataRate", DataRateValue(DataRate(bandwidth * bwMultiplier)));
        p2p.SetDeviceAttribute("Mtu", UintegerValue(1514));
        p2p.SetChannelAttribute("Delay", TimeValue(latency));
        NetDeviceContainer devices = p2p.Install(GetContainerOf({i, j}));
        netdevices[i][j] = devices.Get(0);
        netdevices[j][i] = devices.Get(1);

        // assign ip. Each pair is in their own subnet
        Ipv4AddressHelper ipv4;
        ipv4.SetBase(Ipv4Address(baseAddr), "255.255.255.252");
        Ipv4InterfaceContainer ipif = ipv4.Assign(devices);
        interfaceAddrs[i][j] = ipif.GetAddress(0);
        interfaceAddrs[j][i] = ipif.GetAddress(1);

        // NS_LOG_INFO(ipif.GetAddress(0));

        ffapps[i]->AddPeer(j, interfaceAddrs[j][i], interfaceAddrs[i][j], ffapps[j]);
        ffapps[j]->AddPeer(i, interfaceAddrs[i][j], interfaceAddrs[j][i], ffapps[i]);

        baseAddr += 4;
      }
    }
  }
  // Ipv4GlobalRoutingHelper::PopulateRoutingTables();
}

void FlexFlowDriver::LoadFFTaskGraph(const std::string & taskgraph) {
  std::ifstream t(taskgraph);
  std::string tg_str;
  std::unordered_map<int, Ptr<FlexFlowTask> > tasks;

  t.seekg(0, std::ios::end);   
  tg_str.reserve(t.tellg());
  t.seekg(0, std::ios::beg);

  tg_str.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());    

  auto tg_json = json::parse(tg_str);
  NS_LOG_INFO("Reading dependency graph");

  for (auto & jstask: tg_json["tasks"]) {
    std::string task_type = jstask["type"].get<std::string>();
    // NS_LOG_INFO(task_type);
    Ptr<FlexFlowTask> task;
    int nodeId;
    if (task_type == "inter-communication") {   
      nodeId = jstask["fromNode"].get<int>();         
      task = CreateObject<FlexFlowTask>(ffapps[nodeId], FlexFlowTask::FF_COMM);
      task->fromGuid = jstask["fromTask"].get<int>();
      task->toGuid = jstask["toTask"].get<int>();
      task->fromWorker = jstask["fromWorker"].get<int>();
      task->toWorker = jstask["toWorker"].get<int>();
      task->fromNode = nodeId;
      task->toNode = jstask["toNode"].get<int>();
      task->m_maxBytes = jstask["xferSize"].get<int>();
    } else if (task_type == "intra-communication") { 
      nodeId = jstask["node"].get<int>();         
      task = CreateObject<FlexFlowTask>(ffapps[nodeId], FlexFlowTask::FF_INTRA_COMM);
      task->fromGuid = jstask["fromTask"].get<int>();
      task->toGuid = jstask["toTask"].get<int>();
      task->fromWorker = jstask["fromWorker"].get<int>();
      task->toWorker = jstask["toWorker"].get<int>();
      task->m_maxBytes = jstask["xferSize"].get<int>();
    } else {
      nodeId = jstask["node"].get<int>();         
      task = CreateObject<FlexFlowTask>(ffapps[nodeId], FlexFlowTask::FF_COMP);
    }
    task->guid = jstask["guid"].get<int>();
    task->workerId = jstask["workerId"].get<int>();
    task->readyTime = jstask["readyTime"].get<float>();
    task->startTime = jstask["startTime"].get<float>();
    task->computeTime = jstask["computeTime"].get<float>();

    ffapps[nodeId]->AddTask(task);
    
    tasks[task->guid] = task;
  }

  for (auto & jsedge: tg_json["edges"]) {
    int from = jsedge[0].get<int>();
    int to = jsedge[1].get<int>();
    tasks[from]->AddNextTask(tasks[to]);
    tasks[to]->AddPrevTask(tasks[from]);
  }

  for (auto & app: ffapps) {
    app->SetStartTime(Time("0ns"));
  } 
}

int main(int argc, char * argv[]) {

  // int maxAllowedLink;
  // int interfaceCnt;
  LogComponentEnable("FlexFlowApplication", LOG_LEVEL_INFO);
  LogComponentEnable("FlexFlowDriver", LOG_LEVEL_INFO);

  Time::SetResolution(Time::NS);

  uint32_t numNode = 16;
  uint64_t bandwidth = 10000000000;
  uint32_t tcpSegSize = 1424;
  uint32_t rwnd = 2097152;
  std::string latencyStr = "20ns";
  std::string ffTaskGraph = "ffsim/tg.json";

  CommandLine cmd (__FILE__);
  cmd.AddValue("nNode", "number of nodes", numNode);
  cmd.AddValue("bandwidth", "bandwidth of each link", bandwidth);
  cmd.AddValue("latency", "latency of each link", latencyStr);
  cmd.AddValue("taskGraph", "file of FF task graph", ffTaskGraph);
  cmd.Parse (argc, argv);

  Config::SetDefault("ns3::TcpSocket::SegmentSize", UintegerValue (tcpSegSize));
  Config::SetDefault("ns3::TcpSocket::SndBufSize", UintegerValue (rwnd));
  Config::SetDefault("ns3::TcpSocket::RcvBufSize", UintegerValue (rwnd));
  Config::SetDefault ("ns3::TcpL4Protocol::SocketType", StringValue ("ns3::TcpBic"));

  Time latency = Time(latencyStr);

  NS_LOG_INFO("Building driver");

  FlexFlowDriver ffDriver = FlexFlowDriver(numNode, bandwidth, latency);
  ffDriver.SetFullMeshTopology();
  ffDriver.SetUpNetwork();
  ffDriver.LoadFFTaskGraph(ffTaskGraph);


  NS_LOG_INFO ("Run Simulation.");
  Simulator::Stop (Seconds (30.0));
  Simulator::Run ();
  Simulator::Destroy ();
  NS_LOG_INFO ("Done.");

  return 0;
}