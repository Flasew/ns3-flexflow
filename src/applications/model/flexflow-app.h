#ifndef FLEXFLOW_APP_H
#define FLEXFLOW_APP_H 

#include <unordered_map>
#include <vector>
#include <list>

#include "ns3/application.h"
#include "ns3/address.h"
#include "ns3/traced-callback.h"

namespace ns3 {

class Address;
class Socket;
class FlexFlowTask;
struct PeerFFInfo;

class FlexFlowApplication : public Application {
  friend class FlexFlowTask;
public:

  FlexFlowApplication();
  ~FlexFlowApplication();

  static TypeId GetTypeId();
  // Ptr<Socket> GetSocket();

  void AddTask(Ptr<FlexFlowTask> task);
  void StartInitTasks();
  void AddPeer(int hostId, Address rAddr, Address lAddr, Ptr<FlexFlowApplication> hostApp);

  void NotifyTaskComplete(int taskId);
  // void PrevTaskComplete(int taskId);

protected:
  virtual void DoDispose ();
  virtual void StartApplication ();
  virtual void StopApplication ();

  void HandleRead (Ptr<Socket> socket);
  void HandleAccept (Ptr<Socket> socket, const Address& from);
  void HandlePeerClose (Ptr<Socket> socket);
  void HandlePeerError (Ptr<Socket> socket);

private:

  std::unordered_map<int, PeerFFInfo> peers;
  std::unordered_map<int, Ptr<FlexFlowTask> > tasks;

  int numFinishedTasks = 0;

  uint64_t m_totalRx;

  Ptr<Socket> m_socket; 
  std::list<Ptr<Socket> > m_socketList;

};

struct PeerFFInfo {
  int id;
  Ptr<FlexFlowApplication> app;
  Address peerAddr;
  Address localAddr;
  Ptr<Socket> socket;
};

class FlexFlowTask : public Object {
public:
  enum FlexFlowTaskType {FF_COMM, FF_COMP, FF_INTRA_COMM};

  FlexFlowTask();
  FlexFlowTask(Ptr<FlexFlowApplication> ffapp, FlexFlowTaskType type);
  static TypeId GetTypeId();

  void AddPrevTask(Ptr<FlexFlowTask> task);
  void AddNextTask(Ptr<FlexFlowTask> task);

  void TaskStart();
  void CleanUp();
  void StartFlow();

  void Connect(PeerFFInfo & peer);

  void ConnectionSucceeded(Ptr<Socket> socket);
  void ConnectionFailed(Ptr<Socket> socket);
  void ConnectionClosed(Ptr<Socket> socket);
  void ConnectionClosedError(Ptr<Socket> socket);
  void DataSend(Ptr<Socket> socket, uint32_t);
  void SendData(const Address &from, const Address &to);

  Ptr<FlexFlowApplication> ffapp;
  FlexFlowTaskType type;
  Ptr<Socket> m_socket;
  double readyTime, startTime, computeTime;
  uint64_t m_totBytes, m_maxBytes;
  uint32_t m_sendSize;
  int workerId, guid, fromWorker, toWorker, fromGuid, toGuid, fromNode, toNode;
  bool started, m_connected;
  Ptr<Packet> m_unsentPacket;
  std::unordered_map<int, Ptr<FlexFlowTask> > preTasks; 
  std::vector<Ptr<FlexFlowTask> > nextTasks;
};

}

#endif